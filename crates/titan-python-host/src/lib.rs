//! Injectable cold-path, in-process CPython/Numba compiler host.

use pyo3::{
    Bound, Py, PyAny, Python,
    exceptions::PyValueError,
    types::{PyAnyMethods, PyModule},
};
use std::path::PathBuf;

use titan_runtime_abi::{EVENT_SLOT_COUNT, RuntimeAbiDescriptor};

#[derive(Debug, Clone)]
pub struct StrategySpec {
    pub entrypoint: String,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct StrategyMetadata {
    pub strategy_id: String,
    pub strategy_version: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum PythonHostError {
    #[error("strategy parameters are not valid JSON: {0}")]
    Parameters(#[source] serde_json::Error),
    #[error("Runtime ABI descriptor cannot be serialized: {0}")]
    Abi(#[source] serde_json::Error),
    #[error("Python/Numba strategy compilation failed: {0}")]
    Python(#[from] pyo3::PyErr),
    #[error("compiled strategy returned {actual} callback slots; expected {expected}")]
    CallbackCount { expected: usize, actual: usize },
    #[error("compiled strategy ABI version {actual} does not match Runtime ABI {expected}")]
    AbiVersion { expected: u32, actual: u32 },
    #[error("compiled state {name} must be one-dimensional, C-contiguous {dtype}")]
    InvalidState {
        name: &'static str,
        dtype: &'static str,
    },
}

/// Process-local native strategy handle. It is intentionally neither Clone nor serializable.
pub struct LoadedNumbaStrategy {
    pub metadata: StrategyMetadata,
    pub callback_addresses: [usize; EVENT_SLOT_COUNT],
    pub state_f64_ptr: *mut f64,
    pub state_f64_len: usize,
    pub state_i64_ptr: *mut i64,
    pub state_i64_len: usize,
    keepalive: Py<PyAny>,
}

impl LoadedNumbaStrategy {
    pub fn keepalive(&self) -> &Py<PyAny> {
        &self.keepalive
    }
}

pub trait StrategyCompiler: Send + Sync {
    fn compile(
        &self,
        spec: &StrategySpec,
        abi: &RuntimeAbiDescriptor,
    ) -> Result<LoadedNumbaStrategy, PythonHostError>;
}

#[derive(Debug, Clone)]
pub struct EmbeddedPythonCompiler {
    module: String,
    python_paths: Vec<PathBuf>,
}

impl Default for EmbeddedPythonCompiler {
    fn default() -> Self {
        let python_paths = option_env!("TITAN_PYTHON_SITE_PACKAGES")
            .map(PathBuf::from)
            .into_iter()
            .collect();
        Self {
            module: "titan_strategy.compiler".to_owned(),
            python_paths,
        }
    }
}

impl EmbeddedPythonCompiler {
    pub fn new(module: impl Into<String>) -> Self {
        Self {
            module: module.into(),
            python_paths: Vec::new(),
        }
    }

    /// Prepends import roots before loading the SDK or user strategy. This is deliberately
    /// process-local; a controller must never transfer the resulting function addresses to a
    /// different process.
    pub fn with_python_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.python_paths.push(path.into());
        self
    }
}

impl StrategyCompiler for EmbeddedPythonCompiler {
    fn compile(
        &self,
        spec: &StrategySpec,
        abi: &RuntimeAbiDescriptor,
    ) -> Result<LoadedNumbaStrategy, PythonHostError> {
        let parameters =
            serde_json::to_string(&spec.parameters).map_err(PythonHostError::Parameters)?;
        let abi_json = serde_json::to_string(abi).map_err(PythonHostError::Abi)?;

        Python::attach(|py| {
            if !self.python_paths.is_empty() {
                let sys = PyModule::import(py, "sys")?;
                let path = sys.getattr("path")?;
                for root in self.python_paths.iter().rev() {
                    path.call_method1("insert", (0, root.to_string_lossy().as_ref()))?;
                }
            }
            let json = PyModule::import(py, "json")?;
            let parameters = json.call_method1("loads", (parameters,))?;
            let abi_object = json.call_method1("loads", (abi_json,))?;
            let module = PyModule::import(py, &self.module)?;
            let compiled = module.getattr("compile_strategy")?.call1((
                spec.entrypoint.as_str(),
                parameters,
                abi_object,
            ))?;

            let actual_abi = compiled.getattr("abi_version")?.extract::<u32>()?;
            if actual_abi != abi.abi_version {
                return Err(PythonHostError::AbiVersion {
                    expected: abi.abi_version,
                    actual: actual_abi,
                });
            }

            let addresses = compiled
                .getattr("callback_addresses")?
                .extract::<Vec<usize>>()?;
            let callback_addresses: [usize; EVENT_SLOT_COUNT] =
                addresses.try_into().map_err(|addresses: Vec<usize>| {
                    PythonHostError::CallbackCount {
                        expected: EVENT_SLOT_COUNT,
                        actual: addresses.len(),
                    }
                })?;

            let state_f64 = compiled.getattr("state_f64")?;
            let state_i64 = compiled.getattr("state_i64")?;
            validate_state(&state_f64, "state_f64", "float64")?;
            validate_state(&state_i64, "state_i64", "int64")?;

            Ok(LoadedNumbaStrategy {
                metadata: StrategyMetadata {
                    strategy_id: compiled.getattr("strategy_id")?.extract()?,
                    strategy_version: compiled.getattr("strategy_version")?.extract()?,
                    capabilities: compiled.getattr("capabilities")?.extract()?,
                },
                callback_addresses,
                state_f64_ptr: numpy_address(&state_f64)? as *mut f64,
                state_f64_len: state_f64.len()?,
                state_i64_ptr: numpy_address(&state_i64)? as *mut i64,
                state_i64_len: state_i64.len()?,
                keepalive: compiled.unbind(),
            })
        })
    }
}

fn validate_state(
    value: &Bound<'_, PyAny>,
    name: &'static str,
    dtype: &'static str,
) -> Result<(), PythonHostError> {
    let valid = value.getattr("ndim")?.extract::<usize>()? == 1
        && value
            .getattr("dtype")?
            .getattr("name")?
            .extract::<String>()?
            == dtype
        && value
            .getattr("flags")?
            .get_item("C_CONTIGUOUS")?
            .extract::<bool>()?;
    if valid {
        Ok(())
    } else {
        Err(PythonHostError::InvalidState { name, dtype })
    }
}

fn numpy_address(value: &Bound<'_, PyAny>) -> Result<usize, pyo3::PyErr> {
    let address = value
        .getattr("ctypes")?
        .getattr("data")?
        .extract::<usize>()?;
    if address == 0 {
        Err(PyValueError::new_err("NumPy returned a null state pointer"))
    } else {
        Ok(address)
    }
}

#[cfg(test)]
mod tests {
    use pyo3::ffi::c_str;

    use super::*;

    #[test]
    fn extracts_a_process_local_compiled_descriptor() {
        Python::attach(|py| {
            py.run(
                c_str!(
                    r#"
import sys, types
module = types.ModuleType("fake_titan_compiler")
class DType:
    def __init__(self, name): self.name = name
class CTypes:
    def __init__(self, data): self.data = data
class State:
    ndim = 1
    flags = {"C_CONTIGUOUS": True}
    def __init__(self, name, data, length):
        self.dtype = DType(name)
        self.ctypes = CTypes(data)
        self.length = length
    def __len__(self): return self.length
class Compiled:
    strategy_id = "fake"
    strategy_version = "1.0.0"
    abi_version = 9
    callback_addresses = tuple(range(32))
    capabilities = ("bar",)
    state_f64 = State("float64", 4096, 4)
    state_i64 = State("int64", 8192, 2)
def compile_strategy(entrypoint, parameters, abi): return Compiled()
module.compile_strategy = compile_strategy
sys.modules[module.__name__] = module
"#
                ),
                None,
                None,
            )
        })
        .unwrap();

        let compiler = EmbeddedPythonCompiler::new("fake_titan_compiler");
        let loaded = compiler
            .compile(
                &StrategySpec {
                    entrypoint: "fake:build".to_owned(),
                    parameters: serde_json::json!({}),
                },
                &RuntimeAbiDescriptor::new(Vec::new()),
            )
            .unwrap();
        assert_eq!(loaded.metadata.strategy_id, "fake");
        assert_eq!(loaded.state_f64_ptr as usize, 4096);
        assert_eq!(loaded.state_i64_len, 2);
        assert_eq!(loaded.callback_addresses[31], 31);
    }
}
