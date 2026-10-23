//! End-to-end native compilation through Rust, including its real Numba worker.
use std::{fs, path::Path, process::Command};
fn cli(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(root)
        .args(args)
        .env("TITAN_STRATEGY_SDK", root.join("python/titan-strategy-sdk"))
        .output()
        .unwrap()
}
fn success(output: std::process::Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[cfg(target_os = "linux")]
#[test]
fn native_math_artifact_loads_in_a_clean_host_without_preloaded_libm() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("math.py");
    fs::write(&source, r#"
import numpy as np
from numba import njit
from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import float64, new_state, record
from titan_strategy.types import EventKind, EventQos
state_dtype = record(value=float64)
SPEC = StrategySpec(strategy_id="math_probe", strategy_version="1.0.0", state_schema_version=1,
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA)
@njit
def on_tick(ctx):
    ctx.state["value"] = np.fmod(float(ctx.now), 7.0)
def build(parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick}, {})
"#).unwrap();
    let parameters = directory.path().join("parameters.json");
    fs::write(&parameters, "{}").unwrap();
    let result = success(cli(
        &root,
        &[
            "strategy",
            "compile",
            "--strategy",
            source.to_str().unwrap(),
            "--parameters",
            parameters.to_str().unwrap(),
            "--artifact-format",
            "pair",
            "--output",
            directory.path().join("math").to_str().unwrap(),
            "--json",
        ],
    ));
    let library = result["paths"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|value| value.as_str())
        .find(|path| path.ends_with(".so"))
        .unwrap();
    let probe_source = directory.path().join("probe.c");
    fs::write(
        &probe_source,
        r#"
#include <dlfcn.h>
#include <stdio.h>
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    void *library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (!library) { fprintf(stderr, "%s\n", dlerror()); return 1; }
    dlclose(library);
    return 0;
}
"#,
    )
    .unwrap();
    let probe = directory.path().join("probe");
    let compiled = Command::new("cc")
        .arg(&probe_source)
        .args(["-o"])
        .arg(&probe)
        .arg("-ldl")
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let loaded = Command::new(&probe)
        .arg(library)
        .env_remove("LD_PRELOAD")
        .output()
        .unwrap();
    assert!(
        loaded.status.success(),
        "{}",
        String::from_utf8_lossy(&loaded.stderr)
    );
    let dependencies = Command::new("readelf")
        .args(["-d", library])
        .output()
        .unwrap();
    assert!(dependencies.status.success());
    assert!(String::from_utf8_lossy(&dependencies.stdout).contains("libm.so"));
}
#[test]
fn rust_compiles_signs_and_verifies_native_pair_and_bundle_without_python_verifier() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let pair = directory.path().join("pair/sample");
    let bundle = directory.path().join("bundle/sample.titan");
    let key = directory.path().join("signing-key");
    fs::write(
        &key,
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60\n",
    )
    .unwrap();
    let compile = |path: &Path, format: &str| {
        success(cli(
            &root,
            &[
                "strategy",
                "compile",
                "--strategy",
                "strategies/v13_smoke/strategy.py",
                "--parameters",
                "strategies/v13_smoke/parameters.json",
                "--artifact-format",
                format,
                "--output",
                path.to_str().unwrap(),
                "--signing-key",
                key.to_str().unwrap(),
                "--key-id",
                "test-rfc8032",
                "--json",
            ],
        ))
    };
    let pair_result = compile(&pair, "pair");
    let bundle_result = compile(&bundle, "bundle");
    assert_eq!(
        pair_result["artifact_digest"],
        bundle_result["artifact_digest"]
    );
    assert_eq!(pair_result["native_digest"], bundle_result["native_digest"]);
    let before = fs::read(&bundle).unwrap();
    compile(&bundle, "bundle");
    assert_eq!(fs::read(&bundle).unwrap(), before);
    let verified = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env(
            "TITAN_STRATEGY_PYTHON",
            "/must-not-launch-python-for-verification",
        )
        .args([
            "strategy",
            "validate",
            bundle.to_str().unwrap(),
            "--require-signature",
            "--trusted-key",
            "test-rfc8032=d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(success(verified)["valid"], true);
    assert!(
        !cli(
            &root,
            &["strategy", "validate", bundle.to_str().unwrap(), "--json"]
        )
        .status
        .success()
    );
    // Failed compilation must not replace a prior, verified artifact.
    let rejected = cli(
        &root,
        &[
            "strategy",
            "compile",
            "--strategy",
            "python/titan-strategy-sdk/tests/fixtures/v13_build_side_effect.py",
            "--parameters",
            "strategies/v13_smoke/parameters.json",
            "--output",
            bundle.to_str().unwrap(),
            "--artifact-format",
            "bundle",
            "--json",
        ],
    );
    assert!(!rejected.status.success());
    assert_eq!(fs::read(&bundle).unwrap(), before);
    assert!(
        !fs::read_dir(bundle.parent().unwrap())
            .unwrap()
            .any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".titan-"))
    );
    let wrong = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_STRATEGY_PYTHON", "/must-not-spawn")
        .args([
            "strategy",
            "compile",
            "--strategy",
            "strategies/v13_smoke/strategy.py",
            "--parameters",
            "strategies/v13_smoke/parameters.json",
            "--target",
            "wrong-target",
            "--output",
            bundle.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!wrong.status.success());
    assert_eq!(fs::read(&bundle).unwrap(), before);
}

#[cfg(unix)]
#[test]
fn termination_cancels_compile_process_group_and_cleans_workspace() {
    use std::{
        os::unix::fs::PermissionsExt,
        process::Stdio,
        thread,
        time::{Duration, Instant},
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let worker = directory.path().join("bridge.sh");
    fs::write(
        &worker,
        "#!/bin/sh\ntouch -- \"$0.started\"\n(sleep 0.6; touch -- \"$0.late\") &\nwait\n",
    )
    .unwrap();
    fs::set_permissions(&worker, fs::Permissions::from_mode(0o755)).unwrap();
    let started = directory.path().join("bridge.sh.started");
    let late = directory.path().join("bridge.sh.late");
    let output = directory.path().join("result.titan");
    let mut child = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_STRATEGY_PYTHON", &worker)
        .env("TITAN_STRATEGY_SDK", root.join("python/titan-strategy-sdk"))
        .args([
            "strategy",
            "compile",
            "--strategy",
            "strategies/v13_smoke/strategy.py",
            "--parameters",
            "strategies/v13_smoke/parameters.json",
            "--artifact-format",
            "bundle",
            "--output",
            output.to_str().unwrap(),
            "--json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !started.exists() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("compile worker did not start");
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let deadline = Instant::now() + Duration::from_secs(3);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("compiler did not handle termination");
        }
        thread::sleep(Duration::from_millis(5));
    }
    let result = child.wait_with_output().unwrap();
    assert!(!result.status.success());
    assert!(
        String::from_utf8_lossy(&result.stderr).contains("interrupted")
            || String::from_utf8_lossy(&result.stdout).contains("interrupted")
    );
    thread::sleep(Duration::from_millis(750));
    assert!(!late.exists(), "compiler left a live worker descendant");
    assert!(!output.exists());
    assert!(!fs::read_dir(directory.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".titan-")
    }));
}
