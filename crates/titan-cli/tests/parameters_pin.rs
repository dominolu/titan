//! A release built against a runtime definition pins the stable parameter identity.
//!
//! Artifact bytes are not reproducible (Numba/LLVM payload), so a deployment cannot re-derive the
//! bundle digest. `parameters_digest` is stable, and `titan strategy build --runtime` injects it
//! into the prepared runtime so the runtime refuses activation when the compiled parameters differ
//! from the pin.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn hex(bytes: &[toml::Value]) -> String {
    bytes
        .iter()
        .map(|value| format!("{:02x}", value.as_integer().expect("byte") as u8))
        .collect()
}

#[test]
fn build_with_runtime_pins_the_stable_parameters_digest() {
    let directory =
        std::env::temp_dir().join(format!("titan-parameters-pin-{}", std::process::id()));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).unwrap();
    let releases = directory.join("releases");

    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        // Build inside the temporary directory so `titan.lock` never lands in the repository.
        .current_dir(&directory)
        .env(
            "TITAN_STRATEGY_SDK",
            workspace().join("python/titan-strategy-sdk"),
        )
        .arg("strategy")
        .arg("build")
        .arg("--strategy")
        .arg(workspace().join("strategies/event_counter.py"))
        .arg("--runtime")
        .arg(workspace().join("crates/titan-cli/tests/fixtures/live.toml"))
        .arg("--strategy-key")
        .arg("event_counter")
        .arg("--output")
        .arg(&releases)
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "build failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let release = Path::new(result["release"].as_str().expect("release path"));
    assert!(release.join("runtime.toml").is_file());

    let release_json: serde_json::Value =
        serde_json::from_slice(&fs::read(release.join("release.json")).unwrap()).unwrap();
    let expected = release_json["parameters_digest"].as_str().unwrap();

    let runtime: toml::Value =
        toml::from_str(&fs::read_to_string(release.join("runtime.toml")).unwrap()).unwrap();
    let package = &runtime["strategies"][0]["package"];
    assert_eq!(
        hex(package["parameters_digest"].as_array().expect("pin bytes")),
        expected,
        "the prepared runtime must pin the compiled parameter identity"
    );
    // The artifact digest is still the primary pin; the parameter pin is additive.
    assert_eq!(
        hex(package["expected_digest"].as_array().unwrap()),
        release_json["compiled_strategy_digest"].as_str().unwrap()
    );

    let _ = fs::remove_dir_all(&directory);
}
