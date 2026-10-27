//! Public business observations captured at successful callback boundaries.
//! Private paths and offsets never enter the public schema or its hash.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
};

const MAX_FIELDS: usize = 64;
const MAX_SCHEMA_BYTES: usize = 32_768;
const MAX_DATA_BYTES: usize = 16_384;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BusinessObservationField {
    pub name: String,
    pub kind: String,
    pub unit: String,
    pub label: String,
    pub description: String,
    pub nullable: bool,
    pub enum_values: Vec<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BusinessObservationSchema {
    pub schema_id: String,
    pub schema_version: u32,
    pub fields: Vec<BusinessObservationField>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObservationEquals {
    pub source: String,
    pub equals: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BusinessObservationBinding {
    pub name: String,
    pub source: String,
    pub enum_mapping: BTreeMap<String, String>,
    pub valid_when: Option<ObservationEquals>,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BusinessObservationDefinition {
    pub schema: BusinessObservationSchema,
    pub projection: Vec<BusinessObservationBinding>,
}

#[derive(Clone, Copy, Debug)]
struct ScalarRead {
    offset: usize,
    size: usize,
    kind: u8,
}
#[derive(Clone, Debug)]
struct ObservationRead {
    field: BusinessObservationField,
    read: ScalarRead,
    enum_mapping: BTreeMap<String, String>,
    valid_when: Option<(ScalarRead, String)>,
}
#[derive(Clone, Debug)]
pub struct CompiledBusinessObservation {
    pub schema: BusinessObservationSchema,
    pub schema_hash: String,
    source_schema_hash: [u8; 32],
    source_len: usize,
    reads: Vec<ObservationRead>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BusinessObservationSource {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_sequence: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_committed_sequence: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_sequence: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback_sequence: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_at_ns: Option<String>,
    pub run_generation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BusinessObservationSnapshot {
    pub protocol_version: u32,
    pub schema_id: Option<String>,
    pub schema_version: Option<u32>,
    pub schema_hash: Option<String>,
    pub schema: Option<BusinessObservationSchema>,
    pub available: bool,
    pub complete: bool,
    pub reason: Option<String>,
    pub source: Option<BusinessObservationSource>,
    pub data: BTreeMap<String, Value>,
    pub field_reasons: BTreeMap<String, String>,
}
impl BusinessObservationSnapshot {
    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self {
            protocol_version: 2,
            schema_id: None,
            schema_version: None,
            schema_hash: None,
            schema: None,
            available: false,
            complete: false,
            reason: Some(reason.into()),
            source: None,
            data: BTreeMap::new(),
            field_reasons: BTreeMap::new(),
        }
    }
}
fn invalid() -> V13LoadError {
    V13LoadError::Contract("invalid business observation schema or projection")
}
fn identifier(value: &str, dotted: bool, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.as_bytes()[0].is_ascii_lowercase()
        && value.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || (dotted && b == b'.')
        })
}
fn resolve_scalar(
    schema: &Value,
    path: &str,
    state_len: usize,
) -> Result<ScalarRead, V13LoadError> {
    let mut fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?;
    let mut base = 0usize;
    let mut region_end = state_len;
    let parts: Vec<_> = path.split('.').collect();
    if parts.is_empty() || parts.len() > 16 || path.len() > 256 {
        return Err(invalid());
    }
    for (index, part) in parts.iter().enumerate() {
        if !identifier(part, false, 64) {
            return Err(invalid());
        }
        let field = fields
            .iter()
            .find(|f| f["name"].as_str() == Some(part))
            .ok_or_else(invalid)?;
        if !field["shape"].as_array().ok_or_else(invalid)?.is_empty() {
            return Err(invalid());
        }
        let offset = usize::try_from(field["offset"].as_u64().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        base = base.checked_add(offset).ok_or_else(invalid)?;
        let size = usize::try_from(field["itemsize"].as_u64().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        if base.checked_add(size).is_none_or(|end| end > region_end) {
            return Err(invalid());
        }
        if index + 1 < parts.len() {
            region_end = base + size;
            fields = field["fields"].as_array().ok_or_else(invalid)?;
        } else {
            if field.get("fields").is_some() {
                return Err(invalid());
            }
            let kind = field["type"]
                .as_str()
                .and_then(|s| s.bytes().next())
                .ok_or_else(invalid)?;
            if !matches!(
                (kind, size),
                (b'i' | b'u', 1 | 2 | 4 | 8) | (b'f', 4 | 8) | (b'b', 1)
            ) {
                return Err(invalid());
            }
            let endian = if size == 1 {
                "not_applicable"
            } else {
                "little"
            };
            if field["type"].as_str()
                != Some(format!("{}{size}:{endian}", char::from(kind)).as_str())
            {
                return Err(invalid());
            }
            return Ok(ScalarRead {
                offset: base,
                size,
                kind,
            });
        }
    }
    Err(invalid())
}
impl ScalarRead {
    fn raw(&self, bytes: &[u8]) -> u64 {
        let mut value = [0; 8];
        value[..self.size].copy_from_slice(&bytes[self.offset..self.offset + self.size]);
        u64::from_le_bytes(value)
    }
    fn value(&self, bytes: &[u8]) -> Result<Value, V13LoadError> {
        let bytes = bytes
            .get(self.offset..self.offset.checked_add(self.size).ok_or_else(invalid)?)
            .ok_or_else(invalid)?;
        let text = match (self.kind, self.size) {
            (b'i', 1) => i8::from_le_bytes(bytes.try_into().map_err(|_| invalid())?).to_string(),
            (b'i', 2) => i16::from_le_bytes(bytes.try_into().map_err(|_| invalid())?).to_string(),
            (b'i', 4) => i32::from_le_bytes(bytes.try_into().map_err(|_| invalid())?).to_string(),
            (b'i', 8) => i64::from_le_bytes(bytes.try_into().map_err(|_| invalid())?).to_string(),
            (b'u', 1) => u8::from_le_bytes(bytes.try_into().map_err(|_| invalid())?).to_string(),
            (b'u', 2) => u16::from_le_bytes(bytes.try_into().map_err(|_| invalid())?).to_string(),
            (b'u', 4) => u32::from_le_bytes(bytes.try_into().map_err(|_| invalid())?).to_string(),
            (b'u', 8) => u64::from_le_bytes(bytes.try_into().map_err(|_| invalid())?).to_string(),
            (b'f', 4) => {
                let value = f32::from_le_bytes(bytes.try_into().map_err(|_| invalid())?);
                if !value.is_finite() {
                    return Err(invalid());
                }
                value.to_string()
            }
            (b'f', 8) => {
                let value = f64::from_le_bytes(bytes.try_into().map_err(|_| invalid())?);
                if !value.is_finite() {
                    return Err(invalid());
                }
                value.to_string()
            }
            (b'b', 1) => {
                return match bytes[0] {
                    0 => Ok(false.into()),
                    1 => Ok(true.into()),
                    _ => Err(invalid()),
                };
            }
            _ => return Err(invalid()),
        };
        Ok(text.into())
    }
    fn valid_integer(&self, text: &str) -> bool {
        match self.kind {
            b'i' => text.parse::<i64>().is_ok_and(|value| {
                value.to_string() == text
                    && (self.size == 8
                        || ((-(1i64 << (self.size * 8 - 1)))..(1i64 << (self.size * 8 - 1)))
                            .contains(&value))
            }),
            b'u' => text.parse::<u64>().is_ok_and(|value| {
                value.to_string() == text && (self.size == 8 || value < (1u64 << (self.size * 8)))
            }),
            _ => false,
        }
    }
}
pub(crate) fn compile_business_observation(
    definition: &BusinessObservationDefinition,
    state_schema: &Value,
    identity: RecoveryStateIdentityV13,
) -> Result<CompiledBusinessObservation, V13LoadError> {
    let schema = &definition.schema;
    if !identifier(&schema.schema_id, true, 128)
        || schema.schema_version == 0
        || schema.fields.is_empty()
        || schema.fields.len() > MAX_FIELDS
        || definition.projection.len() != schema.fields.len()
    {
        return Err(invalid());
    }
    let public = serde_json::to_value(schema).map_err(|_| invalid())?;
    let canonical = compiler_json_v13(&public);
    if canonical.len() > MAX_SCHEMA_BYTES {
        return Err(invalid());
    }
    let schema_hash = format!("{:x}", Sha256::digest(&canonical));
    let mut names = BTreeSet::new();
    let mut reads = Vec::new();
    for field in &schema.fields {
        if !identifier(&field.name, false, 64)
            || !names.insert(&field.name)
            || field.unit.is_empty()
            || field.unit.len() > 64
            || field.label.len() > 256
            || field.description.len() > 1024
            || field.enum_values.len() > 64
        {
            return Err(invalid());
        }
        let bindings: Vec<_> = definition
            .projection
            .iter()
            .filter(|p| p.name == field.name)
            .collect();
        if bindings.len() != 1 {
            return Err(invalid());
        }
        let binding = bindings[0];
        let read = resolve_scalar(state_schema, &binding.source, identity.byte_len)?;
        let compatible = match field.kind.as_str() {
            "uint64" => read.kind == b'u',
            "int64" => read.kind == b'i',
            "decimal" => matches!(read.kind, b'i' | b'u' | b'f'),
            "boolean" => read.kind == b'b',
            "enum" => matches!(read.kind, b'i' | b'u') && !binding.enum_mapping.is_empty(),
            _ => false,
        };
        if !compatible {
            return Err(invalid());
        }
        let values: BTreeSet<_> = field.enum_values.iter().collect();
        if values.len() != field.enum_values.len()
            || values.iter().any(|v| !identifier(v, false, 64))
            || (field.kind == "enum"
                && (binding.enum_mapping.len() > 64
                    || binding.enum_mapping.keys().any(|v| !read.valid_integer(v))
                    || binding.enum_mapping.values().collect::<BTreeSet<_>>() != values))
            || (field.kind != "enum" && (!values.is_empty() || !binding.enum_mapping.is_empty()))
        {
            return Err(invalid());
        }
        let valid_when = if let Some(condition) = &binding.valid_when {
            if !field.nullable {
                return Err(invalid());
            }
            let read = resolve_scalar(state_schema, &condition.source, identity.byte_len)?;
            if !read.valid_integer(&condition.equals) {
                return Err(invalid());
            }
            Some((read, condition.equals.clone()))
        } else {
            None
        };
        reads.push(ObservationRead {
            field: field.clone(),
            read,
            enum_mapping: binding.enum_mapping.clone(),
            valid_when,
        });
    }
    Ok(CompiledBusinessObservation {
        schema: schema.clone(),
        schema_hash,
        source_schema_hash: identity.hash,
        source_len: identity.byte_len,
        reads,
    })
}
// Fixed-size, raw scalar values only. Owned by one runtime instance; never persisted.
#[derive(Clone, Copy)]
struct ObservationFrame {
    values: [[u8; 8]; MAX_FIELDS],
    conditions: [[u8; 8]; MAX_FIELDS],
    publication_sequence: u64,
    callback_sequence: u64,
    captured_at_ns: i64,
}
impl Default for ObservationFrame {
    fn default() -> Self {
        Self {
            values: [[0; 8]; MAX_FIELDS],
            conditions: [[0; 8]; MAX_FIELDS],
            publication_sequence: 0,
            callback_sequence: 0,
            captured_at_ns: 0,
        }
    }
}

// All payload words are atomic, so even an interrupted reader has no data race.
// SeqCst orders the version and payload words together: matching even versions
// prove that no writer interleaved with this bounded read. No unsafe seqlock.
struct AtomicObservationFrame {
    version: AtomicU64,
    values: [AtomicU64; MAX_FIELDS],
    conditions: [AtomicU64; MAX_FIELDS],
    callback_sequence: AtomicU64,
    captured_at_ns: AtomicI64,
}
impl Default for AtomicObservationFrame {
    fn default() -> Self {
        Self {
            version: AtomicU64::new(0),
            values: std::array::from_fn(|_| AtomicU64::new(0)),
            conditions: std::array::from_fn(|_| AtomicU64::new(0)),
            callback_sequence: AtomicU64::new(0),
            captured_at_ns: AtomicI64::new(0),
        }
    }
}

/// Single writer under the runtime's existing boundary ownership. Requests
/// coalesce; readers cannot hold up the writer or lose the terminal frame.
pub(crate) struct BusinessObservationPublisher {
    plan: Arc<CompiledBusinessObservation>,
    strategy: StrategyHandle,
    requested: AtomicBool,
    invalid: AtomicBool,
    frame: AtomicObservationFrame,
}
impl BusinessObservationPublisher {
    pub(crate) fn new(
        plan: Arc<CompiledBusinessObservation>,
        strategy: StrategyHandle,
        identity: RecoveryStateIdentityV13,
    ) -> Result<Self, V13LoadError> {
        if plan.source_schema_hash != identity.hash || plan.source_len != identity.byte_len {
            return Err(invalid());
        }
        Ok(Self {
            plan,
            strategy,
            requested: AtomicBool::new(true),
            invalid: AtomicBool::new(false),
            frame: AtomicObservationFrame::default(),
        })
    }
    pub(crate) fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }
    pub(crate) fn invalidate(&self) {
        self.invalid.store(true, Ordering::Release);
    }
    // Hot path: one atomic load without a request; at most 128 scalar copies
    // with a request. No JSON, allocation, filesystem, checksum, lock or retry.
    pub(crate) fn publish(
        &self,
        bytes: &[u8],
        callback_sequence: u64,
        now: impl FnOnce() -> i64,
    ) -> bool {
        if !self.requested.load(Ordering::Acquire) {
            return false;
        }
        // Initialization and recovery seeding are not a successful running callback.
        if callback_sequence == 0 {
            return false;
        }
        if self.invalid.load(Ordering::Acquire) {
            return false;
        }
        let version = self.frame.version.load(Ordering::Relaxed);
        if version != 0 && self.frame.callback_sequence.load(Ordering::Relaxed) == callback_sequence
        {
            return false;
        }
        if bytes.len() != self.plan.source_len || version >= u64::MAX - 1 {
            self.invalidate();
            return false;
        }
        self.requested.swap(false, Ordering::AcqRel);
        // Call external code before making the frame odd. An unwinding clock must
        // not strand readers on a permanently incomplete publication.
        let captured_at_ns = now();
        self.frame.version.store(version + 1, Ordering::SeqCst);
        for (index, item) in self.plan.reads.iter().enumerate() {
            self.frame.values[index].store(item.read.raw(bytes), Ordering::SeqCst);
            if let Some((read, _)) = &item.valid_when {
                self.frame.conditions[index].store(read.raw(bytes), Ordering::SeqCst);
            }
        }
        self.frame
            .callback_sequence
            .store(callback_sequence, Ordering::SeqCst);
        self.frame
            .captured_at_ns
            .store(captured_at_ns, Ordering::SeqCst);
        self.frame.version.store(version + 2, Ordering::SeqCst);
        true
    }
    fn read_frame(&self) -> Option<ObservationFrame> {
        for _ in 0..8 {
            let before = self.frame.version.load(Ordering::SeqCst);
            if before & 1 != 0 {
                continue;
            }
            let mut frame = ObservationFrame {
                publication_sequence: before / 2,
                callback_sequence: self.frame.callback_sequence.load(Ordering::SeqCst),
                captured_at_ns: self.frame.captured_at_ns.load(Ordering::SeqCst),
                ..Default::default()
            };
            for (index, item) in self.plan.reads.iter().enumerate() {
                frame.values[index] = self.frame.values[index]
                    .load(Ordering::SeqCst)
                    .to_le_bytes();
                if item.valid_when.is_some() {
                    frame.conditions[index] = self.frame.conditions[index]
                        .load(Ordering::SeqCst)
                        .to_le_bytes();
                }
            }
            if before == self.frame.version.load(Ordering::SeqCst) {
                return Some(frame);
            }
        }
        None
    }
    pub(crate) fn snapshot(&self) -> Result<BusinessObservationSnapshot, V13LoadError> {
        self.request();
        let Some(frame) = self.read_frame() else {
            return Ok(BusinessObservationSnapshot::unavailable(
                "observation publication busy; retry next sample",
            ));
        };
        if self.invalid.load(Ordering::Acquire) {
            return Ok(BusinessObservationSnapshot::unavailable(
                "runtime observation invalidated",
            ));
        }
        if frame.publication_sequence == 0 {
            return Ok(BusinessObservationSnapshot::unavailable(
                "no successful callback observation published",
            ));
        }
        self.plan.project_frame(&frame, self.strategy.generation)
    }
}
impl CompiledBusinessObservation {
    fn project_frame(
        &self,
        frame: &ObservationFrame,
        generation: u64,
    ) -> Result<BusinessObservationSnapshot, V13LoadError> {
        let mut result = BusinessObservationSnapshot {
            protocol_version: 2,
            schema_id: Some(self.schema.schema_id.clone()),
            schema_version: Some(self.schema.schema_version),
            schema_hash: Some(self.schema_hash.clone()),
            schema: Some(self.schema.clone()),
            available: true,
            complete: true,
            reason: None,
            source: Some(BusinessObservationSource {
                kind: "runtime_publication".into(),
                recovery_sequence: None,
                event_committed_sequence: None,
                publication_sequence: Some(frame.publication_sequence.to_string()),
                callback_sequence: Some(frame.callback_sequence.to_string()),
                captured_at_ns: Some(frame.captured_at_ns.to_string()),
                run_generation: generation.to_string(),
            }),
            data: BTreeMap::new(),
            field_reasons: BTreeMap::new(),
        };
        for (index, item) in self.reads.iter().enumerate() {
            if let Some((read, expected)) = &item.valid_when
                && (ScalarRead { offset: 0, ..*read })
                    .value(&frame.conditions[index])?
                    .as_str()
                    != Some(expected)
            {
                result.data.insert(item.field.name.clone(), Value::Null);
                result
                    .field_reasons
                    .insert(item.field.name.clone(), "condition_not_met".into());
                continue;
            }
            let value = ScalarRead {
                offset: 0,
                ..item.read
            }
            .value(&frame.values[index])
            .and_then(|value| {
                if item.field.kind == "enum" {
                    item.enum_mapping
                        .get(value.as_str().ok_or_else(invalid)?)
                        .cloned()
                        .map(Value::String)
                        .ok_or_else(invalid)
                } else {
                    Ok(value)
                }
            });
            match value {
                Ok(value) => {
                    result.data.insert(item.field.name.clone(), value);
                }
                Err(_) => {
                    result.complete = false;
                    result.data.insert(item.field.name.clone(), Value::Null);
                    result
                        .field_reasons
                        .insert(item.field.name.clone(), "invalid_source_value".into());
                }
            }
        }
        if serde_json::to_vec(&result.data)
            .map_err(|_| invalid())?
            .len()
            > MAX_DATA_BYTES
        {
            return Err(invalid());
        }
        Ok(result)
    }
}

#[cfg(test)]
impl CompiledBusinessObservation {
    fn project(
        &self,
        point: &StrategyRecoveryPoint,
    ) -> Result<BusinessObservationSnapshot, V13LoadError> {
        let publisher = BusinessObservationPublisher::new(
            Arc::new(self.clone()),
            point.strategy,
            RecoveryStateIdentityV13 {
                hash: point.state_schema_hash,
                byte_len: point.state_bytes.len(),
                version: point.state_schema_version,
                alignment: point.state_alignment,
            },
        )?;
        publisher.publish(&point.state_bytes, point.event_committed_sequence, || 123);
        publisher.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    thread_local! {static ALLOCATIONS:Cell<Option<usize>>=const {Cell::new(None)};}
    struct CountingAllocator;
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let _ = ALLOCATIONS.try_with(|count| {
                if let Some(value) = count.get() {
                    count.set(Some(value + 1));
                }
            });
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) };
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let _ = ALLOCATIONS.try_with(|count| {
                if let Some(value) = count.get() {
                    count.set(Some(value + 1));
                }
            });
            unsafe { System.realloc(pointer, layout, size) }
        }
    }
    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;
    fn publisher(fields: usize) -> BusinessObservationPublisher {
        let mut declaration = definition("old", "uint64");
        let field = declaration.schema.fields[0].clone();
        let binding = declaration.projection[0].clone();
        declaration.schema.fields.clear();
        declaration.projection.clear();
        for index in 0..fields {
            let mut field = field.clone();
            let mut binding = binding.clone();
            field.name = format!("value_{index}");
            field.nullable = true;
            binding.name = field.name.clone();
            binding.valid_when = Some(ObservationEquals {
                source: "ready".into(),
                equals: "0".into(),
            });
            declaration.schema.fields.push(field);
            declaration.projection.push(binding);
        }
        let layout = json!({"fields":[{"name":"old","offset":0,"itemsize":8,"type":"u8:little","shape":[]},{"name":"ready","offset":8,"itemsize":1,"type":"u1:not_applicable","shape":[]}]});
        BusinessObservationPublisher::new(
            Arc::new(compile_business_observation(&declaration, &layout, identity(1)).unwrap()),
            point(vec![0; 16], 1).strategy,
            identity(1),
        )
        .unwrap()
    }
    #[test]
    fn publication_coalesces_requests_and_never_replays_a_recovery_point() {
        let publisher = publisher(1);
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&7u64.to_le_bytes());
        assert!(!publisher.snapshot().unwrap().available);
        assert!(publisher.publish(&bytes, 1, || 10));
        // No readers: no timestamp call and no copying on subsequent callbacks.
        assert!(!publisher.publish(&bytes, 2, || panic!("clock on idle publication")));
        for _ in 0..100 {
            publisher.request();
        }
        bytes[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(publisher.publish(&bytes, 3, || 30));
        let observed = publisher.snapshot().unwrap();
        assert_eq!(observed.protocol_version, 2);
        assert_eq!(observed.data["value_0"], u64::MAX.to_string());
        let source = observed.source.unwrap();
        assert_eq!(source.publication_sequence.as_deref(), Some("2"));
        assert_eq!(source.callback_sequence.as_deref(), Some("3"));
        assert_eq!(source.captured_at_ns.as_deref(), Some("30"));
        assert!(source.recovery_sequence.is_none());
        // Re-reading without a callback cannot advance the publication.
        assert!(!publisher.publish(&bytes, 3, || panic!("duplicate boundary")));
        publisher.invalidate();
        assert!(!publisher.snapshot().unwrap().available);
        assert!(!publisher.publish(&bytes, 4, || panic!("failed runtime")));
    }
    #[test]
    fn no_callback_does_not_publish_or_consume_the_initial_request() {
        let publisher = publisher(1);
        assert!(!publisher.publish(&[0; 16], 0, || panic!("no callback")));
        assert!(!publisher.snapshot().unwrap().available);
        assert!(publisher.publish(&[0; 16], 1, || 1));
    }
    #[test]
    fn request_during_capture_remains_pending_for_the_next_boundary() {
        let publisher = Arc::new(publisher(1));
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let writer = {
            let publisher = publisher.clone();
            std::thread::spawn(move || {
                assert!(publisher.publish(&[0; 16], 1, || {
                    entered_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
                    1
                }));
            })
        };
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        // This request is after the writer claimed the previous batch. It must
        // survive even when that writer has not completed its capture yet.
        for _ in 0..100 {
            publisher.request();
        }
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        assert!(publisher.publish(&[0; 16], 2, || 2));
        assert!(!publisher.publish(&[0; 16], 3, || panic!("requests not coalesced")));
    }
    #[test]
    fn busy_reads_are_bounded_and_invalidation_wins_over_an_inflight_writer() {
        let publisher = publisher(64);
        assert!(publisher.publish(&[0; 16], 1, || 1));
        publisher.frame.version.store(3, Ordering::SeqCst);
        assert!(!publisher.snapshot().unwrap().available);
        publisher.frame.version.store(2, Ordering::SeqCst);
        assert!(publisher.publish(&[0; 16], 2, || {
            publisher.invalidate();
            2
        }));
        assert!(!publisher.snapshot().unwrap().available);
        assert!(!publisher.publish(&[0; 16], 3, || panic!("invalidated writer")));
    }
    #[test]
    fn panicking_clock_does_not_leave_an_incomplete_frame() {
        let publisher = publisher(1);
        assert!(publisher.publish(&[0; 16], 1, || 1));
        publisher.request();
        assert!(
            std::panic::catch_unwind(|| {
                publisher.publish(&[0; 16], 2, || panic!("injected clock panic"));
            })
            .is_err()
        );
        assert_eq!(publisher.frame.version.load(Ordering::SeqCst), 2);
        assert!(publisher.snapshot().unwrap().available);
        assert!(publisher.publish(&[0; 16], 3, || 3));
    }
    #[test]
    fn stopped_reader_cannot_block_the_writer_or_terminal_publication() {
        let publisher = Arc::new(publisher(64));
        let first = Arc::new(std::sync::Barrier::new(2));
        let second = Arc::new(std::sync::Barrier::new(2));
        let reader = {
            let publisher = publisher.clone();
            let first = first.clone();
            let second = second.clone();
            std::thread::spawn(move || {
                let before = publisher.frame.version.load(Ordering::SeqCst);
                first.wait();
                second.wait();
                // The old partial read must be rejected, even if this reader was
                // paused while the final writer ran and no further callbacks exist.
                assert_ne!(before, publisher.frame.version.load(Ordering::SeqCst));
                let snapshot = publisher.snapshot().unwrap();
                assert_eq!(
                    snapshot.source.unwrap().callback_sequence.as_deref(),
                    Some("2")
                );
            })
        };
        first.wait();
        assert!(publisher.publish(&[0; 16], 1, || 10));
        publisher.request();
        assert!(publisher.publish(&[0; 16], 2, || 20));
        second.wait();
        reader.join().unwrap();
    }
    #[test]
    fn concurrent_readers_only_see_whole_published_frames() {
        let publisher = Arc::new(publisher(64));
        let mut initial = [0; 16];
        initial[..8].copy_from_slice(&1u64.to_le_bytes());
        assert!(publisher.publish(&initial, 1, || 1));
        let finished = Arc::new(AtomicBool::new(false));
        let started = Arc::new(std::sync::Barrier::new(4));
        let readers: Vec<_> = (0..3)
            .map(|_| {
                let publisher = publisher.clone();
                let finished = finished.clone();
                let started = started.clone();
                std::thread::spawn(move || {
                    assert!(publisher.snapshot().unwrap().available);
                    let mut observed = 1;
                    started.wait();
                    while !finished.load(Ordering::Acquire) {
                        let value = publisher.snapshot().unwrap();
                        if value.available {
                            observed += 1;
                            let source = value.source.unwrap();
                            let expected = source.callback_sequence.unwrap();
                            assert_eq!(source.captured_at_ns.as_deref(), Some(expected.as_str()));
                            let nullable = expected.parse::<u64>().unwrap() % 2 == 0;
                            assert!(value.data.values().all(|v| if nullable {
                                v.is_null()
                            } else {
                                v.as_str() == Some(expected.as_str())
                            }));
                        }
                    }
                    observed
                })
            })
            .collect();
        // Give every reader an actual accepted initial sample; a vacuous stress
        // run with no accepted frames would not validate the seqlock.
        started.wait();
        for sequence in 2..20000u64 {
            let mut bytes = [0; 16];
            bytes[..8].copy_from_slice(&sequence.to_le_bytes());
            bytes[8] = u8::from(sequence % 2 == 0);
            publisher.request();
            publisher.publish(&bytes, sequence, || sequence as i64);
            if sequence % 127 == 0 {
                // Readers are allowed to return busy under continuous writes.
                // Periodic stable windows make accepted updates non-vacuous even
                // on a single CPU, alongside the overlapping-copy stress above.
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        }
        finished.store(true, Ordering::Release);
        let observed: usize = readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum();
        assert!(observed > 3, "no concurrent update was accepted");
        println!("coherent concurrent observations accepted: {observed}");
    }
    #[test]
    fn publication_performance_probe() {
        use std::{hint::black_box, time::Instant};
        for fields in [8, 64] {
            let publisher = publisher(fields);
            let bytes = [0; 16];
            publisher.publish(&bytes, 1, || 1);
            let count = 100000u64;
            ALLOCATIONS.with(|value| value.set(Some(0)));
            let start = Instant::now();
            for sequence in 2..count + 2 {
                black_box(publisher.publish(black_box(&bytes), sequence, || 1));
            }
            let idle_ns = start.elapsed().as_nanos() as f64 / count as f64;
            let samples = 10000u64;
            let mut elapsed = [0u128; 10000];
            for sequence in 0..samples {
                publisher.request();
                let start = Instant::now();
                black_box(publisher.publish(black_box(&bytes), sequence + count + 2, || 1));
                elapsed[sequence as usize] = start.elapsed().as_nanos();
            }
            let allocations = ALLOCATIONS.with(|value| value.replace(None).unwrap());
            assert_eq!(allocations, 0, "publication must not allocate");
            elapsed.sort_unstable();
            println!(
                "{}",
                json!({"probe":"business_observation_publication","fields":fields,"idle_ns_per_callback":idle_ns,"capture_p50_ns":elapsed[5000],"capture_p99_ns":elapsed[9900],"capture_max_ns":elapsed[9999],"hot_path_allocations":allocations,"max_scalar_bytes":fields*16})
            );
        }
    }
    fn point(bytes: Vec<u8>, hash: u8) -> StrategyRecoveryPoint {
        StrategyRecoveryPoint {
            checkpoint_id: 42,
            strategy: StrategyHandle {
                runtime_strategy_no: RuntimeStrategyNo(7),
                generation: 2,
            },
            strategy_instance_id: 7,
            strategy_type: "test".into(),
            source_version: "1.0.0".into(),
            generation: 2,
            event_committed_sequence: 80,
            compiled_strategy_digest: [1; 32],
            binding_digest: [2; 32],
            abi_version: 13,
            state_schema_version: 1,
            state_schema_hash: [hash; 32],
            state_alignment: 8,
            state_bytes: bytes.into(),
            orders_list: Vec::new().into(),
            orders_list_checksum: [0; 32],
            public_state_identity: [0; 32],
            checksum: [0; 32],
            recovery: None,
        }
    }
    fn definition(source: &str, kind: &str) -> BusinessObservationDefinition {
        serde_json::from_value(json!({"schema":{"schema_id":"titan.test","schema_version":1,"fields":[{"name":"value","kind":kind,"unit":"count","label":"value","description":"","nullable":false,"enum_values":[]}]},"projection":[{"name":"value","source":source,"enum_mapping":{},"valid_when":null}]})).unwrap()
    }
    fn schema(name: &str, offset: usize, kind: &str) -> Value {
        json!({"fields":[{"name":name,"offset":offset,"itemsize":8,"type":kind,"shape":[]}]})
    }
    fn identity(hash: u8) -> RecoveryStateIdentityV13 {
        RecoveryStateIdentityV13 {
            version: 1,
            hash: [hash; 32],
            byte_len: 16,
            alignment: 8,
        }
    }
    #[test]
    fn public_schema_is_independent_of_private_name_offset_and_hash() {
        let one = compile_business_observation(
            &definition("old", "uint64"),
            &schema("old", 0, "u8:little"),
            identity(1),
        )
        .unwrap();
        let two = compile_business_observation(
            &definition("new", "uint64"),
            &schema("new", 8, "u8:little"),
            identity(2),
        )
        .unwrap();
        assert_eq!(one.schema_hash, two.schema_hash);
        let mut first = vec![0; 16];
        first[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        let mut second = vec![0; 16];
        second[8..].copy_from_slice(&u64::MAX.to_le_bytes());
        let first = one.project(&point(first, 1)).unwrap();
        let second = two.project(&point(second, 2)).unwrap();
        assert_eq!(first.data, second.data);
        assert_eq!(first.data["value"], u64::MAX.to_string());
        assert!(
            !serde_json::to_string(&first)
                .unwrap()
                .contains("state_bytes")
        );
        assert!(one.project(&point(vec![0; 16], 2)).is_err());
        let public = serde_json::to_string(&one.schema).unwrap();
        assert!(!public.contains("source"));
        assert!(!public.contains("offset"));
    }
    #[test]
    fn projection_rejects_unknown_paths_outside_bounds_and_incompatible_types() {
        assert!(
            compile_business_observation(
                &definition("missing", "uint64"),
                &schema("old", 0, "u8:little"),
                identity(1)
            )
            .is_err()
        );
        assert!(
            compile_business_observation(
                &definition("old", "uint64"),
                &schema("old", 12, "u8:little"),
                identity(1)
            )
            .is_err()
        );
        assert!(
            compile_business_observation(
                &definition("old", "uint64"),
                &schema("old", 0, "f8:little"),
                identity(1)
            )
            .is_err()
        );
        assert!(
            compile_business_observation(
                &definition("old", "uint64"),
                &schema("old", 0, "u8:big"),
                identity(1)
            )
            .is_err()
        );
    }
    #[test]
    fn reads_preserve_u64_and_reject_non_finite_values() {
        let read = ScalarRead {
            offset: 0,
            size: 8,
            kind: b'u',
        };
        assert_eq!(
            read.value(&u64::MAX.to_le_bytes()).unwrap(),
            u64::MAX.to_string()
        );
        assert!(
            ScalarRead { kind: b'f', ..read }
                .value(&f64::NAN.to_le_bytes())
                .is_err()
        );
    }
    #[test]
    fn projection_is_bounded_and_does_not_publish_recovery_internals() {
        let mut value = definition("old", "uint64");
        value.projection.push(value.projection[0].clone());
        assert!(
            compile_business_observation(&value, &schema("old", 0, "u8:little"), identity(1))
                .is_err()
        );
        let mut value = definition("old", "uint64");
        value.schema.fields[0].unit = String::new();
        assert!(
            compile_business_observation(&value, &schema("old", 0, "u8:little"), identity(1))
                .is_err()
        );
    }
    #[test]
    fn missing_initialization_is_nullable_and_unknown_enum_is_incomplete() {
        let layout = json!({"fields":[{"name":"old","offset":0,"itemsize":8,"type":"u8:little","shape":[]},
            {"name":"ready","offset":8,"itemsize":1,"type":"u1:not_applicable","shape":[]}]});
        let mut value = definition("old", "uint64");
        value.schema.fields[0].nullable = true;
        value.projection[0].valid_when = Some(ObservationEquals {
            source: "ready".into(),
            equals: "1".into(),
        });
        let plan = compile_business_observation(&value, &layout, identity(1)).unwrap();
        let output = plan.project(&point(vec![0; 16], 1)).unwrap();
        assert!(output.complete);
        assert!(output.data["value"].is_null());
        let mut value = definition("old", "enum");
        value.schema.fields[0].enum_values = vec!["running".into()];
        value.projection[0]
            .enum_mapping
            .insert("1".into(), "running".into());
        let plan = compile_business_observation(&value, &layout, identity(1)).unwrap();
        let output = plan.project(&point(vec![0; 16], 1)).unwrap();
        assert!(!output.complete);
        assert!(output.data["value"].is_null());
        assert_eq!(output.field_reasons["value"], "invalid_source_value");
    }
}
