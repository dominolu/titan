use std::{path::PathBuf, process::Command};

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn live_fixture() -> &'static str {
    "crates/titan-cli/tests/fixtures/live.toml"
}

#[test]
fn v13_live_validation_needs_no_python_runtime() {
    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args([
            "validate",
            "event_counter",
            "-e",
            "live",
            "-m",
            "tick",
            "-c",
            live_fixture(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"valid\n");
}
