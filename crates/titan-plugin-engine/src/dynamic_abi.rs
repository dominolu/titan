use crate::{ApiVersion, ErrorKind, PluginError, engine_error};

pub const TITAN_PLUGIN_MAGIC: u64 = 0x5449_5441_4E50_4C47;

/// Versioned, fixed-layout dynamic plugin entry descriptor. No Rust-owned value crosses this ABI.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PluginApiV1 {
    pub magic: u64,
    pub struct_size: u32,
    pub abi_major: u16,
    pub abi_minor: u16,
    pub manifest_schema_major: u16,
    pub manifest_schema_minor: u16,
    pub required_feature_bits: u64,
    pub optional_feature_bits: u64,
    pub manifest_json: Option<unsafe extern "C" fn(*mut usize) -> *const u8>,
    pub create: Option<unsafe extern "C" fn(*const u8, usize, *mut u64) -> i32>,
    pub destroy: Option<unsafe extern "C" fn(u64) -> i32>,
    pub last_error: Option<unsafe extern "C" fn(*mut u8, usize) -> usize>,
}

impl PluginApiV1 {
    pub fn validate(
        &self,
        host_abi: ApiVersion,
        supported_features: u64,
        manifest_schema_major: u16,
    ) -> Result<(), PluginError> {
        if self.magic != TITAN_PLUGIN_MAGIC {
            return Err(engine_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "invalid plugin magic",
            ));
        }
        if self.struct_size < std::mem::size_of::<Self>() as u32 {
            return Err(engine_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "plugin descriptor is truncated",
            ));
        }
        if self.abi_major != host_abi.major || self.abi_minor > host_abi.minor {
            return Err(engine_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "dynamic plugin ABI is incompatible",
            ));
        }
        if self.manifest_schema_major != manifest_schema_major {
            return Err(engine_error(
                ErrorKind::ManifestSchemaMismatch,
                "validate_dynamic_abi",
                "manifest schema major is incompatible",
            ));
        }
        if self.required_feature_bits & !supported_features != 0 {
            return Err(engine_error(
                ErrorKind::UnsupportedAbiFeature,
                "validate_dynamic_abi",
                "plugin requires unsupported ABI features",
            ));
        }
        if self.manifest_json.is_none()
            || self.create.is_none()
            || self.destroy.is_none()
            || self.last_error.is_none()
        {
            return Err(engine_error(
                ErrorKind::AbiVersionMismatch,
                "validate_dynamic_abi",
                "required function pointer is null",
            ));
        }
        Ok(())
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TitanBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
    pub free: Option<unsafe extern "C" fn(*mut u8, usize, usize)>,
}
