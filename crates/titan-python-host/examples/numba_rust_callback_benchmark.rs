use std::{
    hint::black_box,
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use titan_python_host::{EmbeddedPythonCompiler, StrategyCompiler, StrategySpec};
use titan_runtime_abi::{StrategyRuntimeContext, runtime_abi_descriptor};

type Callback = unsafe extern "C" fn(*mut StrategyRuntimeContext) -> i32;

unsafe extern "C" fn rust_tick(context: *mut StrategyRuntimeContext) -> i32 {
    // SAFETY: the benchmark owns a live context and one-element state buffer for every call.
    let context = unsafe { &mut *context };
    // SAFETY: state_f64_ptr points at the one-element Rust buffer installed below.
    unsafe { *context.state_f64_ptr += 1.0 };
    0
}

#[derive(serde::Serialize)]
struct Summary {
    implementation: &'static str,
    samples: usize,
    calls_per_sample: usize,
    total_calls: usize,
    p50_ns_per_call: f64,
    p99_ns_per_call: f64,
    p999_ns_per_call: f64,
    max_ns_per_call: f64,
}

#[derive(serde::Serialize)]
struct Report {
    schema_version: u32,
    abi_version: u32,
    profile: &'static str,
    target: String,
    os: String,
    cpu: String,
    rust: Summary,
    numba: Summary,
    numba_over_rust_p50: f64,
    numba_over_rust_p99: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if cfg!(debug_assertions) {
        return Err("run this benchmark with `cargo run --release -p titan-python-host --example numba_rust_callback_benchmark`".into());
    }
    let samples = env_usize("TITAN_BENCH_SAMPLES", 10_000)?;
    let calls_per_sample = env_usize("TITAN_BENCH_CALLS_PER_SAMPLE", 1_000)?;
    let warmup_calls = env_usize("TITAN_BENCH_WARMUP_CALLS", 1_000_000)?;
    if samples == 0 || calls_per_sample == 0 || warmup_calls == 0 {
        return Err("benchmark sample, batch and warmup counts must be positive".into());
    }

    let fixture = FixtureDirectory::create()?;
    let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../python/titan-strategy-sdk");
    let loaded = EmbeddedPythonCompiler::default()
        .with_python_path(fixture.path.clone())
        .with_python_path(sdk)
        .compile(
            &StrategySpec {
                entrypoint: "titan_callback_bench:build".into(),
                parameters: serde_json::json!({}),
            },
            &runtime_abi_descriptor(),
        )?;
    let address = loaded.callback_addresses[6];
    if address == 0 {
        return Err("compiled Numba package did not export on_tick".into());
    }
    // SAFETY: EmbeddedPythonCompiler validated the callback table and the loaded keepalive remains
    // owned until both benchmark runs finish.
    let numba_tick: Callback = unsafe { std::mem::transmute(address) };

    let rust = measure("rust", rust_tick, samples, calls_per_sample, warmup_calls)?;
    let numba = measure("numba", numba_tick, samples, calls_per_sample, warmup_calls)?;
    let report = Report {
        schema_version: 1,
        abi_version: runtime_abi_descriptor().abi_version,
        profile: "release",
        target: format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
        os: command_output("uname", &["-a"]),
        cpu: cpu_description(),
        numba_over_rust_p50: numba.p50_ns_per_call / rust.p50_ns_per_call,
        numba_over_rust_p99: numba.p99_ns_per_call / rust.p99_ns_per_call,
        rust,
        numba,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn measure(
    implementation: &'static str,
    callback: Callback,
    samples: usize,
    calls_per_sample: usize,
    warmup_calls: usize,
) -> Result<Summary, Box<dyn std::error::Error>> {
    let mut state = [0.0_f64];
    let mut context = StrategyRuntimeContext {
        state_f64_ptr: state.as_mut_ptr(),
        state_f64_len: state.len(),
        ..StrategyRuntimeContext::default()
    };
    for _ in 0..warmup_calls {
        // SAFETY: callback and context satisfy the stable Strategy ABI contract.
        if unsafe { callback(black_box(&mut context)) } != 0 {
            return Err("strategy callback returned an error during warmup".into());
        }
    }
    let mut elapsed = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        for _ in 0..calls_per_sample {
            // SAFETY: callback and context satisfy the stable Strategy ABI contract.
            if unsafe { callback(black_box(&mut context)) } != 0 {
                return Err("strategy callback returned an error during measurement".into());
            }
        }
        elapsed.push(started.elapsed().as_nanos() as u64);
    }
    let expected = (warmup_calls + samples * calls_per_sample) as f64;
    if state[0] != expected {
        return Err(format!(
            "callback state mismatch: expected {expected}, got {}",
            state[0]
        )
        .into());
    }
    elapsed.sort_unstable();
    let per_call = |permille: usize| {
        let index = (elapsed.len() * permille).div_ceil(1_000).saturating_sub(1);
        elapsed[index] as f64 / calls_per_sample as f64
    };
    Ok(Summary {
        implementation,
        samples,
        calls_per_sample,
        total_calls: samples * calls_per_sample,
        p50_ns_per_call: per_call(500),
        p99_ns_per_call: per_call(990),
        p999_ns_per_call: per_call(999),
        max_ns_per_call: *elapsed.last().unwrap() as f64 / calls_per_sample as f64,
    })
}

fn env_usize(name: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn command_output(program: &str, args: &[&str]) -> String {
    std::process::Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unavailable".into())
}

fn cpu_description() -> String {
    if cfg!(target_os = "macos") {
        command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|value| {
                value.lines().find_map(|line| {
                    line.strip_prefix("model name\t:")
                        .map(|value| value.trim().to_owned())
                })
            })
            .unwrap_or_else(|| "unavailable".into())
    }
}

struct FixtureDirectory {
    path: PathBuf,
}

impl FixtureDirectory {
    fn create() -> Result<Self, std::io::Error> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("titan-numba-rust-bench-{nonce}"));
        std::fs::create_dir(&path)?;
        std::fs::write(
            path.join("titan_callback_bench.py"),
            r#"import numpy as np
from numba import njit

@njit
def on_tick(s):
    s.state[0] += 1.0

def build(_parameters):
    return {
        "strategy_id": "callback-benchmark",
        "strategy_version": "1.0.0",
        "on_tick": on_tick,
        "state": np.zeros(1, dtype=np.float64),
        "state_i64": np.zeros(1, dtype=np.int64),
    }
"#,
        )?;
        Ok(Self { path })
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
