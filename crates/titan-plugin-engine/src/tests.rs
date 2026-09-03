use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use semver::{Version, VersionReq};

use crate::*;

#[test]
fn dynamic_buffer_uses_producer_release_on_success_and_rejection() {
    static RELEASES: AtomicUsize = AtomicUsize::new(0);
    static OVERSIZED_RELEASES: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn release(data: *mut u8, _len: usize, capacity: usize) {
        RELEASES.fetch_add(1, Ordering::Relaxed);
        if !data.is_null() && capacity != 0 {
            // SAFETY: this fixture receives the allocation tuple produced by `make_buffer`.
            drop(unsafe { Vec::from_raw_parts(data, 0, capacity) });
        }
    }

    unsafe extern "C" fn release_oversized(_data: *mut u8, _len: usize, _capacity: usize) {
        OVERSIZED_RELEASES.fetch_add(1, Ordering::Relaxed);
    }

    fn make_buffer(value: &[u8]) -> TitanBuffer {
        let mut bytes = value.to_vec();
        let buffer = TitanBuffer {
            data: bytes.as_mut_ptr(),
            len: bytes.len(),
            capacity: bytes.capacity(),
            free: Some(release),
        };
        std::mem::forget(bytes);
        buffer
    }

    RELEASES.store(0, Ordering::Relaxed);
    let valid = make_buffer(b"titan");
    // SAFETY: `make_buffer` returns a readable allocation and its matching release callback.
    assert_eq!(unsafe { valid.copy_and_free() }.unwrap(), b"titan");

    let mut invalid = make_buffer(b"abi");
    invalid.len = invalid.capacity + 1;
    // SAFETY: the malformed length is intentional; the producer callback still owns and releases
    // the exact allocation tuple, and the host must reject it before attempting a read.
    assert_eq!(
        unsafe { invalid.copy_and_free() }.unwrap_err().kind,
        ErrorKind::PluginFailed
    );
    assert_eq!(RELEASES.load(Ordering::Relaxed), 2);

    OVERSIZED_RELEASES.store(0, Ordering::Relaxed);
    let oversized = TitanBuffer {
        data: std::ptr::NonNull::<u8>::dangling().as_ptr(),
        len: isize::MAX as usize + 1,
        capacity: isize::MAX as usize + 1,
        free: Some(release_oversized),
    };
    // SAFETY: the deliberately invalid range is rejected before it is read; the no-op fixture
    // callback only proves that producer ownership is still discharged on rejection.
    assert_eq!(
        unsafe { oversized.copy_and_free() }.unwrap_err().kind,
        ErrorKind::PluginFailed
    );
    assert_eq!(OVERSIZED_RELEASES.load(Ordering::Relaxed), 1);
}

#[cfg(unix)]
#[test]
fn dynamic_plugin_loads_package_runs_lifecycle_and_keeps_code_lease() {
    use std::{fs, process::Command};

    use sha2::{Digest, Sha256};

    let unique = format!(
        "titan-dynamic-plugin-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let directory = std::env::temp_dir().join(unique);
    fs::create_dir(&directory).unwrap();
    let source = directory.join("fixture.c");
    let library = if cfg!(target_os = "macos") {
        directory.join("libfixture.dylib")
    } else {
        directory.join("libfixture.so")
    };
    fs::write(
        &source,
        r#"
#include <string.h>
#include "titan_plugin_abi_v1.h"

static const uint8_t MANIFEST[] = "{\"plugin_type\":\"fixture\",\"name\":\"fixture\",\"version\":\"1.2.3\",\"engine_api_version\":{\"major\":2,\"minor\":0},\"abi_version\":{\"major\":1,\"minor\":0},\"config_schema_version\":1,\"config_schema\":{},\"provides\":[],\"requires\":[],\"publishes\":[],\"subscribes\":[],\"supported_execution_models\":[\"Passive\"],\"reload_policy\":\"Never\"}";
static const uint64_t INTERFACE_VALUE = 0xfeedbeef;
static const uint8_t ERROR_TEXT[] = "fixture failure";
static const uint8_t* manifest_json(size_t* length) { *length = sizeof(MANIFEST) - 1; return MANIFEST; }
static TitanStatus create(const uint8_t* config, size_t length, TitanPluginHandle* out) {
  if (!config || length < 2 || !out) return 1; *out = 7; return 0;
}
static TitanStatus destroy(TitanPluginHandle handle) { return handle == 7 ? 0 : 1; }
static size_t last_error(uint8_t* output, size_t capacity) {
  size_t length = sizeof(ERROR_TEXT) - 1; if (length > capacity) length = capacity;
  if (output && length) memcpy(output, ERROR_TEXT, length); return length;
}
static TitanStatus validate(TitanPluginHandle handle) { return handle == 7 ? 0 : 1; }
static TitanStatus start(TitanPluginHandle handle, const TitanHostApiV1* host) {
  if (handle != 7 || !host || host->magic != 0x544954414e484f53ULL || !host->now_ns) return 1;
  return host->now_ns(host->context) == 42 ? 0 : 1;
}
static TitanStatus quiesce(TitanPluginHandle handle, uint32_t reason) { return handle == 7 && reason <= 2 ? 0 : 1; }
static TitanStatus stop(TitanPluginHandle handle) { return handle == 7 ? 0 : 1; }
static TitanStatus query_interface(TitanPluginHandle handle, const uint8_t* name, size_t length, uint16_t major, const void** out) {
  static const char expected[] = "fixture.api";
  if (handle != 7 || major != 1 || length != sizeof(expected)-1 || memcmp(name, expected, length) || !out) return 1;
  *out = &INTERFACE_VALUE; return 0;
}
static const PluginApiV1 API = {
  TITAN_PLUGIN_MAGIC, sizeof(PluginApiV1), TITAN_DYNAMIC_ABI_MAJOR, TITAN_DYNAMIC_ABI_MINOR,
  TITAN_MANIFEST_SCHEMA_MAJOR, TITAN_MANIFEST_SCHEMA_MINOR, 0, 0,
  manifest_json, create, destroy, last_error, validate, start, quiesce, stop, query_interface
};
#if defined(_WIN32)
__declspec(dllexport)
#else
__attribute__((visibility("default")))
#endif
const PluginApiV1* titan_plugin_entry_v1(void) { return &API; }
"#,
    )
    .unwrap();
    let mut compiler = Command::new(std::env::var_os("CC").unwrap_or_else(|| "cc".into()));
    if cfg!(target_os = "macos") {
        compiler.arg("-dynamiclib");
    } else {
        compiler.args(["-shared", "-fPIC"]);
    }
    let status = compiler
        .arg("-I")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/include"))
        .arg("-O2")
        .arg(&source)
        .arg("-o")
        .arg(&library)
        .status()
        .unwrap();
    assert!(status.success());

    let digest = format!("{:x}", Sha256::digest(fs::read(&library).unwrap()));
    let package_manifest = directory.join("plugin.json");
    let mut manifest_value = serde_json::json!({
        "schema_major": 1,
        "schema_minor": 0,
        "package_version": "1.2.3",
        "library": library.file_name().unwrap().to_string_lossy(),
        "sha256": "0".repeat(64),
    });
    fs::write(
        &package_manifest,
        vec![b' '; MAX_DYNAMIC_MANIFEST_BYTES + 1],
    )
    .unwrap();
    let oversized_manifest = DynamicPluginLoader::default()
        .load_package(&package_manifest)
        .err()
        .unwrap();
    assert_eq!(oversized_manifest.kind, ErrorKind::ManifestInvalid);
    assert!(oversized_manifest.message.contains("size limit"));
    fs::write(
        &package_manifest,
        serde_json::to_vec(&manifest_value).unwrap(),
    )
    .unwrap();
    assert_eq!(
        DynamicPluginLoader::default()
            .load_package(&package_manifest)
            .err()
            .unwrap()
            .kind,
        ErrorKind::ManifestInvalid
    );
    manifest_value["sha256"] = serde_json::Value::String(digest);
    manifest_value["package_version"] = serde_json::Value::String("9.9.9".into());
    fs::write(
        &package_manifest,
        serde_json::to_vec(&manifest_value).unwrap(),
    )
    .unwrap();
    assert_eq!(
        DynamicPluginLoader::default()
            .load_package(&package_manifest)
            .err()
            .unwrap()
            .kind,
        ErrorKind::ManifestInvalid
    );
    manifest_value["package_version"] = serde_json::Value::String("1.2.3".into());
    fs::write(
        &package_manifest,
        serde_json::to_vec(&manifest_value).unwrap(),
    )
    .unwrap();

    let package = DynamicPluginLoader::default()
        .load_package(&package_manifest)
        .unwrap();
    assert_eq!(package.package_version, Version::new(1, 2, 3));
    assert_eq!(package.code.manifest()["plugin_type"], "fixture");
    let mut incompatible = package.code.api();
    incompatible.required_feature_bits = 1;
    assert_eq!(
        incompatible
            .validate_descriptor(ApiVersion::new(1, 0), 0, 1)
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedAbiFeature
    );
    let lifecycle_factory = DynamicLifecyclePluginFactory::from_package(LoadedDynamicPackage {
        manifest_path: package.manifest_path.clone(),
        package_version: package.package_version.clone(),
        code: package.code.clone(),
    })
    .unwrap();
    assert_eq!(lifecycle_factory.manifest().plugin_type.as_ref(), "fixture");
    let code_lease = package.code.clone();
    let mut instance = package.code.create(br#"{}"#).unwrap();
    assert_eq!(
        instance.query_interface("fixture.api", 1).unwrap_err().kind,
        ErrorKind::RuntimeNotActive
    );
    instance.validate().unwrap();
    instance.validate().unwrap();
    unsafe extern "C" fn now(_: *mut std::ffi::c_void) -> i64 {
        42
    }
    let host = TitanHostApiV1 {
        magic: TITAN_HOST_MAGIC,
        struct_size: std::mem::size_of::<TitanHostApiV1>() as u32,
        abi_major: 1,
        abi_minor: 0,
        context: std::ptr::null_mut(),
        publish_event: None,
        log: None,
        now_ns: Some(now),
        resolve_secret: None,
    };
    // SAFETY: `host` remains alive until after the instance is stopped below.
    unsafe { instance.start(&host) }.unwrap();
    assert_eq!(
        // SAFETY: same host-lifetime invariant as the successful start above.
        unsafe { instance.start(&host) }.unwrap_err().kind,
        ErrorKind::RuntimeNotActive
    );
    let interface = instance.query_interface("fixture.api", 1).unwrap();
    // SAFETY: the fixture interface is a static u64 retained by `code_lease`.
    assert_eq!(unsafe { *interface.cast::<u64>() }, 0xfeedbeef);
    instance.quiesce(TITAN_STOP_SHUTDOWN).unwrap();
    instance.quiesce(TITAN_STOP_SHUTDOWN).unwrap();
    instance.stop().unwrap();
    instance.stop().unwrap();
    drop(package);
    assert!(code_lease.path().is_file());
    drop(instance);
    drop(code_lease);
    fs::remove_dir_all(directory).unwrap();
}

struct TestPlugin {
    log: Arc<Mutex<Vec<String>>>,
    identity: PluginIdentity,
    fail_start: bool,
}

impl Plugin for TestPlugin {
    fn validate(&self, _: &ValidationContext) -> Result<(), PluginError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("validate:{}", self.identity.instance_id));
        Ok(())
    }
    fn start(&mut self, _: &mut PluginContext) -> Result<(), PluginError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("start:{}", self.identity.instance_id));
        if self.fail_start {
            Err(PluginError::new(
                ErrorKind::RuntimeStartFailed,
                self.identity.clone(),
                LifecycleState::Starting,
                "start",
                "injected failure",
            ))
        } else {
            Ok(())
        }
    }
    fn quiesce(&mut self, _: StopReason) -> Result<(), PluginError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("quiesce:{}", self.identity.instance_id));
        Ok(())
    }
    fn stop(&mut self) -> Result<(), PluginError> {
        self.log
            .lock()
            .unwrap()
            .push(format!("stop:{}", self.identity.instance_id));
        Ok(())
    }
}

struct TestFactory {
    manifest: &'static PluginManifest,
    log: Arc<Mutex<Vec<String>>>,
    fail_instance: Option<Arc<str>>,
}

impl PluginFactory for TestFactory {
    fn manifest(&self) -> &'static PluginManifest {
        self.manifest
    }
    fn create(&self, init: PluginInit) -> Result<PluginBundle, PluginError> {
        Ok(PluginBundle {
            lifecycle: Box::new(TestPlugin {
                log: self.log.clone(),
                fail_start: self.fail_instance.as_deref() == Some(&init.identity.instance_id),
                identity: init.identity,
            }),
            service_exports: Vec::new(),
            subscription_bindings: Vec::new(),
        })
    }
}

fn manifest(
    plugin_type: &str,
    provides: Vec<ProvidedService>,
    requires: Vec<RequiredService>,
) -> &'static PluginManifest {
    Box::leak(Box::new(PluginManifest {
        plugin_type: Arc::from(plugin_type),
        name: Arc::from(plugin_type),
        version: Version::new(1, 0, 0),
        engine_api_version: CORE_RUNTIME_API_VERSION,
        abi_version: ApiVersion::new(1, 0),
        config_schema_version: 1,
        config_schema: Arc::new(serde_json::json!({})),
        provides,
        requires,
        publishes: Vec::new(),
        subscribes: Vec::new(),
        supported_execution_models: BTreeSet::from([ExecutionModel::Passive]),
        reload_policy: ReloadPolicy::RestartRequired,
    }))
}

fn spec(plugin_type: &str, instance_id: &str) -> PluginSpec {
    PluginSpec {
        instance_id: Arc::from(instance_id),
        plugin_type: Arc::from(plugin_type),
        config: Arc::new(ConfigSnapshot::new(1, serde_json::json!({}))),
        enabled: true,
        execution: ExecutionSpec {
            model: ExecutionModel::Passive,
            cpu_affinity: None,
            callback_budget: None,
        },
        subscription_limits: SubscriptionLimits {
            max_capacity: 64,
            allowed_qos: BTreeSet::from([
                EventQos::Latest,
                EventQos::ReliableOrdered,
                EventQos::BestEffort,
            ]),
        },
        service_scopes: Vec::new(),
        required_service_scopes: Vec::new(),
    }
}

fn service(name: &str) -> ProvidedService {
    ProvidedService {
        id: ServiceId::new("test", name),
        version: Version::new(1, 0, 0),
        scope_kind: ScopeKind::Global,
        call_mode: CallMode::Inline,
    }
}

fn requirement(name: &str, required: bool) -> RequiredService {
    RequiredService {
        id: ServiceId::new("test", name),
        version: VersionReq::parse("^1").unwrap(),
        scope_kind: ScopeKind::Global,
        required,
    }
}

#[test]
fn passive_plugins_cannot_declare_event_subscriptions() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let item = Box::leak(Box::new(PluginManifest {
        subscribes: vec![SubscribedEvent {
            event_type: Arc::from("test.event"),
            schema_version: 1,
            allowed_qos: BTreeSet::from([EventQos::ReliableOrdered]),
        }],
        ..manifest("passive-subscriber", vec![], vec![]).clone()
    }));
    let mut registry = PluginRegistry::default();
    registry
        .register(
            Arc::new(TestFactory {
                manifest: item,
                log,
                fail_instance: None,
            }),
            Version::new(1, 0, 0),
            "test",
            CORE_RUNTIME_API_VERSION,
            ApiVersion::new(1, 0),
        )
        .unwrap();
    assert_eq!(
        compile_plugin_plan(&[spec("passive-subscriber", "one")], &registry, 1)
            .unwrap_err()
            .kind,
        ErrorKind::ConfigInvalid
    );
}

#[test]
fn concrete_custom_scopes_select_the_correct_provider_instance() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let custom_service = ServiceId::new("test", "scoped");
    let provider_manifest = manifest(
        "scoped-provider",
        vec![ProvidedService {
            id: custom_service.clone(),
            version: Version::new(1, 0, 0),
            scope_kind: ScopeKind::Custom,
            call_mode: CallMode::Inline,
        }],
        vec![],
    );
    let consumer_manifest = manifest(
        "scoped-consumer",
        vec![],
        vec![RequiredService {
            id: custom_service.clone(),
            version: VersionReq::parse("^1").unwrap(),
            scope_kind: ScopeKind::Custom,
            required: true,
        }],
    );
    let mut registry = PluginRegistry::default();
    for item in [provider_manifest, consumer_manifest] {
        registry
            .register(
                Arc::new(TestFactory {
                    manifest: item,
                    log: log.clone(),
                    fail_instance: None,
                }),
                Version::new(1, 0, 0),
                "test",
                CORE_RUNTIME_API_VERSION,
                ApiVersion::new(1, 0),
            )
            .unwrap();
    }
    let scope_a = ServiceScope::Custom {
        namespace: Arc::from("account"),
        key: Arc::from("a"),
    };
    let scope_b = ServiceScope::Custom {
        namespace: Arc::from("account"),
        key: Arc::from("b"),
    };
    let mut provider_a = spec("scoped-provider", "provider-a");
    provider_a.service_scopes = vec![(custom_service.clone(), scope_a.clone())];
    let mut provider_b = spec("scoped-provider", "provider-b");
    provider_b.service_scopes = vec![(custom_service.clone(), scope_b.clone())];
    let mut consumer = spec("scoped-consumer", "consumer-b");
    consumer.required_service_scopes = vec![(custom_service, scope_b)];
    let plan = compile_plugin_plan(&[provider_a, provider_b, consumer], &registry, 1).unwrap();
    assert_eq!(
        plan.entry("consumer-b").unwrap().bindings[0]
            .provider
            .as_ref()
            .unwrap()
            .instance_id
            .as_ref(),
        "provider-b"
    );
    assert_ne!(
        plan.entry("consumer-b").unwrap().bindings[0]
            .key
            .as_ref()
            .unwrap()
            .scope,
        scope_a
    );
}

#[test]
fn compiles_dependencies_in_topological_order_and_reverse_stop_order() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let provider = manifest("provider", vec![service("orders")], vec![]);
    let consumer = manifest("consumer", vec![], vec![requirement("orders", true)]);
    let mut registry = PluginRegistry::default();
    registry
        .register(
            Arc::new(TestFactory {
                manifest: provider,
                log: log.clone(),
                fail_instance: None,
            }),
            Version::new(1, 0, 0),
            "test",
            CORE_RUNTIME_API_VERSION,
            ApiVersion::new(1, 0),
        )
        .unwrap();
    registry
        .register(
            Arc::new(TestFactory {
                manifest: consumer,
                log,
                fail_instance: None,
            }),
            Version::new(1, 0, 0),
            "test",
            CORE_RUNTIME_API_VERSION,
            ApiVersion::new(1, 0),
        )
        .unwrap();
    let plan = compile_plugin_plan(
        &[
            spec("consumer", "consumer-1"),
            spec("provider", "provider-1"),
        ],
        &registry,
        7,
    )
    .unwrap();
    assert_eq!(
        plan.start_order(),
        &[
            Arc::<str>::from("provider-1"),
            Arc::<str>::from("consumer-1")
        ]
    );
    assert_eq!(
        plan.stop_order(),
        &[
            Arc::<str>::from("consumer-1"),
            Arc::<str>::from("provider-1")
        ]
    );
    assert!(plan.entry("consumer-1").unwrap().bindings[0].direct_inline);
}

#[test]
fn rejects_missing_dependency_and_dependency_cycle() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let missing = manifest("missing", vec![], vec![requirement("none", true)]);
    let a = manifest("a", vec![service("a")], vec![requirement("b", true)]);
    let b = manifest("b", vec![service("b")], vec![requirement("a", true)]);
    let mut registry = PluginRegistry::default();
    for item in [missing, a, b] {
        registry
            .register(
                Arc::new(TestFactory {
                    manifest: item,
                    log: log.clone(),
                    fail_instance: None,
                }),
                Version::new(1, 0, 0),
                "test",
                CORE_RUNTIME_API_VERSION,
                ApiVersion::new(1, 0),
            )
            .unwrap();
    }
    assert_eq!(
        compile_plugin_plan(&[spec("missing", "m")], &registry, 1)
            .unwrap_err()
            .kind,
        ErrorKind::DependencyMissing
    );
    assert_eq!(
        compile_plugin_plan(&[spec("a", "a1"), spec("b", "b1")], &registry, 1)
            .unwrap_err()
            .kind,
        ErrorKind::DependencyCycle
    );
}

struct AddOne;
impl ServiceEndpoint for AddOne {
    fn call(&self, request: BoxValue, _: TraceContext) -> Result<BoxValue, PluginError> {
        Ok(Box::new(*request.downcast::<u64>().unwrap() + 1))
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[test]
fn stable_service_handle_observes_gate_unavailability_and_generation_replacement() {
    let identity = PluginIdentity::new("provider", "one");
    let key = ServiceKey {
        id: ServiceId::new("test", "add"),
        version: Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    };
    let recorder = Arc::new(FlightRecorder::new(32));
    let metrics = Arc::new(PluginEngineMetrics::default());
    let mut registry = ServiceRegistry::new(recorder.clone(), metrics.clone());
    registry.stage(key.clone(), identity).unwrap();
    let handle = registry.bind(&key).unwrap();
    assert_eq!(
        handle
            .call(Box::new(1_u64), TraceContext::default())
            .unwrap_err()
            .kind,
        ErrorKind::ServiceUnavailable
    );
    let gate = Arc::new(ActivationGate::new());
    registry
        .publish(&key, Arc::new(AddOne), gate.clone())
        .unwrap();
    assert_eq!(
        handle
            .call(Box::new(1_u64), TraceContext::default())
            .unwrap_err()
            .kind,
        ErrorKind::RuntimeNotActive
    );
    gate.activate();
    assert_eq!(
        *handle
            .call(Box::new(1_u64), TraceContext::default())
            .unwrap()
            .downcast::<u64>()
            .unwrap(),
        2
    );
    let first_generation = handle.generation().unwrap();
    let replacement_gate = Arc::new(ActivationGate::new());
    replacement_gate.activate();
    registry
        .publish(&key, Arc::new(AddOne), replacement_gate)
        .unwrap();
    assert!(handle.generation().unwrap() > first_generation);
    registry.make_unavailable(&key);
    assert_eq!(
        handle
            .call(Box::new(1_u64), TraceContext::default())
            .unwrap_err()
            .kind,
        ErrorKind::ServiceUnavailable
    );
    let records = recorder.freeze();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.kind == trace_kind::SERVICE_BEGIN)
            .count(),
        4
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.kind == trace_kind::SERVICE_END)
            .count(),
        4
    );
    assert!(
        records
            .iter()
            .filter(|record| record.kind == trace_kind::SERVICE_END)
            .any(|record| record.force_keep)
    );
    let metric = metrics.service_snapshots().pop().unwrap();
    assert_eq!(metric.call_success_total, 1);
    assert_eq!(metric.call_failure_total, 3);
    assert_eq!(metric.unavailable_total, 3);
}

#[test]
fn resource_scope_closes_in_reverse_order() {
    let identity = PluginIdentity::new("test", "scope");
    let mut scope = ResourceScope::new(identity.clone());
    let log = Arc::new(Mutex::new(Vec::new()));
    for value in [1, 2, 3] {
        let log = log.clone();
        scope
            .handle()
            .register(
                "closure",
                ClosureResource(Some(move || {
                    log.lock().unwrap().push(value);
                    Ok(())
                })),
            )
            .unwrap();
    }
    scope.close().unwrap();
    assert_eq!(&*log.lock().unwrap(), &[3, 2, 1]);
}

#[test]
fn registering_into_a_closed_scope_immediately_releases_the_resource() {
    let identity = PluginIdentity::new("test", "closed-scope");
    let mut scope = ResourceScope::new(identity);
    let handle = scope.handle();
    scope.close().unwrap();
    let released = Arc::new(AtomicUsize::new(0));
    let observed = released.clone();
    assert_eq!(
        handle
            .register(
                "late",
                ClosureResource(Some(move || {
                    observed.fetch_add(1, Ordering::Release);
                    Ok(())
                })),
            )
            .unwrap_err()
            .kind,
        ErrorKind::ResourceReleaseFailed
    );
    assert_eq!(released.load(Ordering::Acquire), 1);
}

struct CountingHandler(AtomicUsize);
impl ControlHandler for CountingHandler {
    fn execute(&self, _: &ControlOperation) -> Result<(), PluginError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn control_commands_are_idempotent_and_queryable() {
    let handler = Arc::new(CountingHandler(AtomicUsize::new(0)));
    let control = PluginControl::new(2, handler.clone()).unwrap();
    let command = ControlCommand {
        idempotency_key: Arc::from("same"),
        deadline: deadline_after(Duration::from_secs(1)),
        operation: ControlOperation::StopAll,
    };
    let first = control.try_submit(command.clone()).unwrap();
    let second = control.try_submit(command).unwrap();
    assert_eq!(first, second);
    let limit = std::time::Instant::now() + Duration::from_secs(1);
    while !control.query(first.request_id).unwrap().is_terminal()
        && std::time::Instant::now() < limit
    {
        std::thread::yield_now();
    }
    assert!(matches!(
        control.query(first.request_id),
        Some(ControlOperationState::Succeeded)
    ));
    assert_eq!(handler.0.load(Ordering::Relaxed), 1);
}

#[test]
fn activation_gate_wakes_all_waiters_without_lost_notification() {
    let gate = Arc::new(ActivationGate::new());
    let mut threads = Vec::new();
    for _ in 0..8 {
        let gate = gate.clone();
        threads.push(std::thread::spawn(move || gate.wait_until_active()));
    }
    assert!(gate.activate());
    for thread in threads {
        assert_eq!(thread.join().unwrap(), ActivationState::Active);
    }
}

#[test]
fn change_plan_honors_reload_policy_and_execution_changes() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut live = manifest("live", vec![], vec![]).clone();
    live.reload_policy = ReloadPolicy::Live;
    let live = Box::leak(Box::new(live));
    let mut registry = PluginRegistry::default();
    registry
        .register(
            Arc::new(TestFactory {
                manifest: live,
                log,
                fail_instance: None,
            }),
            Version::new(1, 0, 0),
            "test",
            CORE_RUNTIME_API_VERSION,
            ApiVersion::new(1, 0),
        )
        .unwrap();
    let old = spec("live", "one");
    let mut changed = old.clone();
    changed.config = Arc::new(ConfigSnapshot {
        schema_version: 1,
        version: 2,
        hash: Arc::from("new"),
        loaded_at: std::time::SystemTime::now(),
        source: Arc::from("test"),
        value: Arc::new(serde_json::json!({"x": 2})),
    });
    let old_plan = compile_plugin_plan(&[old], &registry, 1).unwrap();
    let new_plan = compile_plugin_plan(&[changed], &registry, 2).unwrap();
    assert_eq!(
        compile_change_plan(&old_plan, &new_plan, &registry)
            .unwrap()
            .changes[0]
            .kind,
        ChangeKind::Live
    );
}

#[test]
fn callback_monitor_detects_budget_and_stalls_and_flight_recorder_is_bounded() {
    let monitor = CallbackMonitor::default();
    monitor.register(
        "handler",
        CallbackBudget {
            soft_budget_us: 1,
            stall_threshold_us: 10,
            max_consecutive_violations: 2,
        },
    );
    let guard = monitor.begin("handler").unwrap();
    std::thread::sleep(Duration::from_micros(50));
    assert_eq!(
        monitor.scan_stalled(std::time::Instant::now()),
        vec![Arc::<str>::from("handler")]
    );
    guard.finish();
    let stats = monitor.stats("handler").unwrap();
    assert_eq!(stats.total, 1);
    assert_eq!(stats.budget_exceeded, 1);
    assert!(stats.running_since.is_none());
    let second = monitor.begin("handler").unwrap();
    std::thread::sleep(Duration::from_micros(50));
    second.finish();
    assert_eq!(
        monitor.callbacks_over_violation_limit(),
        vec![Arc::<str>::from("handler")]
    );
    let heartbeat = ThreadHeartbeat::new();
    let now = std::time::Instant::now() + Duration::from_millis(5);
    assert!(heartbeat.is_stale(now, Duration::from_millis(1)));
    heartbeat.beat();
    assert!(!heartbeat.is_stale(std::time::Instant::now(), Duration::from_millis(1)));
    let recorder = FlightRecorder::new(2);
    for value in 1..=3 {
        recorder.record(
            TraceContext {
                trace_id: value,
                causation_id: 0,
            },
            1,
            value,
            false,
        );
    }
    let frozen = recorder.freeze();
    assert_eq!(frozen.len(), 2);
    assert_eq!(frozen[0].value, 2);
    assert_eq!(frozen[1].value, 3);
}

#[test]
fn cold_and_blocking_executors_enforce_bounded_admission() {
    let cold = ColdAsyncRuntime::new(1, 1).unwrap();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let _task = cold
        .try_spawn(async move {
            let _ = release_rx.await;
            7_u64
        })
        .unwrap();
    assert_eq!(
        cold.try_spawn(async { 8_u64 }).unwrap_err().kind,
        ErrorKind::ControlQueueFull
    );
    release_tx.send(()).unwrap();
    let blocking = BlockingExecutor::new(1, 1).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let next = count.clone();
    blocking
        .try_submit(move || {
            next.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    let limit = std::time::Instant::now() + Duration::from_secs(1);
    while count.load(Ordering::Relaxed) == 0 && std::time::Instant::now() < limit {
        std::thread::yield_now();
    }
    assert_eq!(count.load(Ordering::Relaxed), 1);
}

#[test]
fn dedicated_executor_waits_for_activation_and_rejects_an_unavailable_cpu() {
    let identity = PluginIdentity::new("test", "dedicated");
    let mut scope = ResourceScope::new(identity.clone());
    let gate = Arc::new(ActivationGate::new());
    let (ran_tx, ran_rx) = std::sync::mpsc::sync_channel(1);
    spawn_dedicated(
        identity.clone(),
        &scope.handle(),
        "titan-test-dedicated",
        None,
        gate.clone(),
        move || ran_tx.send(()).unwrap(),
    )
    .unwrap();
    assert!(ran_rx.recv_timeout(Duration::from_millis(10)).is_err());
    assert!(gate.activate());
    ran_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    scope.close().unwrap();

    let mut rejected_scope = ResourceScope::new(identity.clone());
    let error = spawn_dedicated(
        identity,
        &rejected_scope.handle(),
        "titan-test-invalid-cpu",
        Some(usize::MAX),
        Arc::new(ActivationGate::new()),
        || {},
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::ConfigInvalid);
    assert_eq!(rejected_scope.resource_count(), 0);
    rejected_scope.close().unwrap();
}

#[test]
fn plugin_plan_accepts_each_declared_execution_model_and_validates_budgets() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let all_models = Box::leak(Box::new(PluginManifest {
        supported_execution_models: BTreeSet::from([
            ExecutionModel::Dedicated,
            ExecutionModel::Background,
            ExecutionModel::Passive,
        ]),
        ..manifest("execution-models", vec![], vec![]).clone()
    }));
    let mut registry = PluginRegistry::default();
    registry
        .register(
            Arc::new(TestFactory {
                manifest: all_models,
                log,
                fail_instance: None,
            }),
            Version::new(1, 0, 0),
            "test",
            CORE_RUNTIME_API_VERSION,
            ApiVersion::new(1, 0),
        )
        .unwrap();

    for (index, model) in [
        ExecutionModel::Dedicated,
        ExecutionModel::Background,
        ExecutionModel::Passive,
    ]
    .into_iter()
    .enumerate()
    {
        let mut value = spec("execution-models", &format!("model-{index}"));
        value.execution.model = model;
        value.execution.callback_budget = Some(CallbackBudget {
            soft_budget_us: 10,
            stall_threshold_us: 40,
            max_consecutive_violations: 2,
        });
        let plan = compile_plugin_plan(&[value], &registry, index as u64 + 1).unwrap();
        assert_eq!(plan.entries().next().unwrap().spec.execution.model, model);
    }

    let mut invalid = spec("execution-models", "invalid-budget");
    invalid.execution.model = ExecutionModel::Dedicated;
    invalid.execution.callback_budget = Some(CallbackBudget {
        soft_budget_us: 10,
        stall_threshold_us: 9,
        max_consecutive_violations: 1,
    });
    assert_eq!(
        compile_plugin_plan(&[invalid], &registry, 4)
            .unwrap_err()
            .kind,
        ErrorKind::ConfigInvalid
    );
}

#[test]
fn endpoint_lease_keeps_old_generation_alive_during_replacement_model() {
    loom::model(|| {
        use loom::sync::{Arc as LoomArc, RwLock};
        let slot = LoomArc::new(RwLock::new(LoomArc::new(1_u64)));
        let reader_slot = slot.clone();
        let reader = loom::thread::spawn(move || {
            let lease = reader_slot.read().unwrap().clone();
            let observed = *lease;
            loom::thread::yield_now();
            assert_eq!(*lease, observed);
        });
        *slot.write().unwrap() = LoomArc::new(2);
        reader.join().unwrap();
        assert_eq!(**slot.read().unwrap(), 2);
    });
}

struct AddService;
impl Service for AddService {
    type Request = u64;
    type Response = u64;
}
struct TypedAdd;
impl TypedServiceEndpoint<AddService> for TypedAdd {
    fn call(&self, request: u64, _: TraceContext) -> Result<u64, PluginError> {
        Ok(request + 1)
    }
}

#[test]
fn typed_service_handle_uses_prebound_adapter() {
    let identity = PluginIdentity::new("typed", "provider");
    let key = ServiceKey {
        id: ServiceId::new("test", "typed-add"),
        version: Version::new(1, 0, 0),
        scope: ServiceScope::Global,
    };
    let mut registry = ServiceRegistry::default();
    registry.stage(key.clone(), identity).unwrap();
    let untyped = registry.bind(&key).unwrap();
    let services = BoundServices::new(BTreeMap::from([(key.clone(), untyped)]));
    let handle = services.require::<AddService>(&key).unwrap();
    let gate = Arc::new(ActivationGate::new());
    gate.activate();
    registry
        .publish(&key, typed_endpoint::<AddService, _>(TypedAdd), gate)
        .unwrap();
    assert_eq!(handle.call(41, TraceContext::default()).unwrap(), 42);
}

#[test]
fn plugin_configuration_is_validated_against_manifest_schema() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut constrained = manifest("constrained", vec![], vec![]).clone();
    constrained.config_schema = Arc::new(
        serde_json::json!({"type":"object", "required":["value"], "properties":{"value":{"type":"integer"}}}),
    );
    let constrained = Box::leak(Box::new(constrained));
    let mut registry = PluginRegistry::default();
    registry
        .register(
            Arc::new(TestFactory {
                manifest: constrained,
                log,
                fail_instance: None,
            }),
            Version::new(1, 0, 0),
            "test",
            CORE_RUNTIME_API_VERSION,
            ApiVersion::new(1, 0),
        )
        .unwrap();
    assert_eq!(
        compile_plugin_plan(&[spec("constrained", "bad")], &registry, 1)
            .unwrap_err()
            .kind,
        ErrorKind::ConfigInvalid
    );

    let mut wrong_version = spec("constrained", "wrong-version");
    wrong_version.config =
        Arc::new(ConfigSnapshot::new(2, serde_json::json!({"value": 1})).with_schema_version(2));
    assert_eq!(
        compile_plugin_plan(&[wrong_version], &registry, 1)
            .unwrap_err()
            .kind,
        ErrorKind::ConfigInvalid
    );
}
