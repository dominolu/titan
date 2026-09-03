use connector::dynamic_plugin::{self, DynamicVenue};
use titan_plugin_engine::{
    PluginApiV1, TITAN_DYNAMIC_ABI_VERSION, TITAN_MANIFEST_SCHEMA_MAJOR,
    TITAN_MANIFEST_SCHEMA_MINOR, TITAN_PLUGIN_MAGIC, TitanPluginHandle, TitanStatus,
};

static MANIFEST: &[u8] = br#"{"plugin_type":"connector.binance-futures","name":"Binance Futures Connector Plugin","version":"0.1.0","engine_api_version":{"major":2,"minor":0},"abi_version":{"major":1,"minor":0},"config_schema_version":1,"config_schema":{"type":"object"},"provides":[],"requires":[],"publishes":[],"subscribes":[],"supported_execution_models":["Passive"],"reload_policy":"RestartRequired"}"#;

unsafe extern "C" fn manifest(length: *mut usize) -> *const u8 {
    if length.is_null() {
        return std::ptr::null();
    }
    unsafe { length.write(MANIFEST.len()) };
    MANIFEST.as_ptr()
}

unsafe extern "C" fn create(
    input: *const u8,
    length: usize,
    output: *mut TitanPluginHandle,
) -> TitanStatus {
    unsafe {
        dynamic_plugin::create_root_from_json(DynamicVenue::BinanceFutures, input, length, output)
    }
}

static API: PluginApiV1 = PluginApiV1 {
    magic: TITAN_PLUGIN_MAGIC,
    struct_size: std::mem::size_of::<PluginApiV1>() as u32,
    abi_major: TITAN_DYNAMIC_ABI_VERSION.major,
    abi_minor: TITAN_DYNAMIC_ABI_VERSION.minor,
    manifest_schema_major: TITAN_MANIFEST_SCHEMA_MAJOR,
    manifest_schema_minor: TITAN_MANIFEST_SCHEMA_MINOR,
    required_feature_bits: 0,
    optional_feature_bits: 0,
    manifest_json: Some(manifest),
    create: Some(create),
    destroy: Some(dynamic_plugin::destroy_root),
    last_error: Some(dynamic_plugin::last_error),
    validate: Some(dynamic_plugin::validate_root),
    start: Some(dynamic_plugin::start_root),
    quiesce: Some(dynamic_plugin::quiesce_root),
    stop: Some(dynamic_plugin::stop_root),
    query_interface: Some(dynamic_plugin::query_interface),
};

#[unsafe(no_mangle)]
pub extern "C" fn titan_plugin_entry_v1() -> *const PluginApiV1 {
    &API
}
