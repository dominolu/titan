use std::{
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn run(fixture: &str) -> serde_json::Value {
    let root = workspace();
    let titan_home = std::env::temp_dir().join(format!(
        "titan-cli-test-{}-{}",
        std::process::id(),
        fixture.replace('.', "-")
    ));
    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", titan_home)
        .args(["run", &format!("crates/titan-cli/tests/fixtures/{fixture}")])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn bar_tick_and_hybrid_follow_frozen_callback_counts() {
    let bar = run("dual_ma_run.json");
    assert_eq!(bar["market_event_count"], 5);
    assert_eq!(bar["callback_count"][5], 5);
    assert_eq!(bar["state_f64"][2], 13.5);
    assert_eq!(bar["state_f64"][3], 12.5);
    assert_eq!(bar["order_count"], 0);
    assert!(bar["execution_reports"].is_array());
    assert!(bar["funding_reports"].is_array());
    assert!(bar["exchange_final"].is_array());
    assert!(bar["local_delivered_final"].is_array());
    assert!(bar["returns"].is_array());

    let tick = run("tick_run.json");
    assert_eq!(tick["market_event_count"], 12);
    assert_eq!(tick["callback_count"][6], 6);
    assert_eq!(tick["state_f64"][0], 12.0);

    let hybrid = run("hybrid_run.json");
    assert_eq!(hybrid["market_event_count"], 17);
    assert_eq!(hybrid["callback_count"][5], 5);
    assert_eq!(hybrid["state_f64"][0], 12.0);
    assert_eq!(hybrid["state_f64"][2], 5.0);
}

#[test]
fn compile_failure_has_stable_controller_exit_and_failed_registry_state() {
    let root = workspace();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let titan_home =
        std::env::temp_dir().join(format!("titan-cli-failure-{}-{nonce}", std::process::id()));
    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args([
            "run",
            "crates/titan-cli/tests/fixtures/compile_failure_run.json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(31));
    let listing = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(root)
        .env("TITAN_HOME", titan_home)
        .args(["ls", "--json"])
        .output()
        .unwrap();
    let runs: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    assert_eq!(runs[0]["state"], "FAILED");
    assert!(
        runs[0]["error"]
            .as_str()
            .unwrap()
            .contains("strategy compilation failed")
    );
}

#[test]
fn report_verifies_the_rust_authored_bundle_before_reading_it() {
    let root = workspace();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let titan_home =
        std::env::temp_dir().join(format!("titan-cli-report-{}-{nonce}", std::process::id()));
    let run = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args(["run", "crates/titan-cli/tests/fixtures/dual_ma_run.json"])
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let listing = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args(["ls", "--json"])
        .output()
        .unwrap();
    let rows: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    let run_id = rows[0]["id"].as_str().unwrap();
    let report = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args(["report", run_id])
        .output()
        .unwrap();
    assert!(report.status.success());
    let result: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert!(result["execution_reports"].is_array());

    let result_path = PathBuf::from(rows[0]["result_path"].as_str().unwrap());
    std::fs::write(&result_path, b"{}\n").unwrap();
    let tampered = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(root)
        .env("TITAN_HOME", titan_home)
        .args(["report", run_id])
        .output()
        .unwrap();
    assert!(!tampered.status.success());
    assert!(String::from_utf8_lossy(&tampered.stderr).contains("integrity mismatch"));
}

#[test]
fn live_run_spec_dry_run_needs_no_connector_or_python() {
    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args([
            "validate",
            "crates/titan-cli/tests/fixtures/live_dry_run.json",
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

#[cfg(unix)]
#[test]
fn foreground_sigint_is_forwarded_and_live_worker_stops_cleanly() {
    let root = workspace();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let titan_home =
        std::env::temp_dir().join(format!("titan-cli-signal-{}-{nonce}", std::process::id()));
    let mut controller = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args(["run", "crates/titan-cli/tests/fixtures/live_dry_run.json"])
        .stdout(Stdio::null())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(30);
    let run_id = loop {
        let listing = Command::new(env!("CARGO_BIN_EXE_titan"))
            .current_dir(&root)
            .env("TITAN_HOME", &titan_home)
            .args(["ls", "--json"])
            .output()
            .unwrap();
        if listing.status.success() {
            let rows: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
            if let Some(run) = rows.as_array().and_then(|rows| rows.first())
                && run["state"] == "RUNNING"
            {
                break run["id"].as_str().unwrap().to_owned();
            }
        }
        assert!(Instant::now() < deadline, "worker did not enter RUNNING");
        thread::sleep(Duration::from_millis(50));
    };

    // Safety: this is the exact child process created by this test.
    assert_eq!(
        unsafe { libc::kill(controller.id() as i32, libc::SIGINT) },
        0
    );
    loop {
        if let Some(status) = controller.try_wait().unwrap() {
            assert!(
                status.success(),
                "foreground controller exited with {status}"
            );
            break;
        }
        if Instant::now() >= deadline {
            let _ = Command::new(env!("CARGO_BIN_EXE_titan"))
                .current_dir(&root)
                .env("TITAN_HOME", &titan_home)
                .args(["stop", &run_id])
                .status();
            let _ = controller.kill();
            panic!("foreground controller did not stop before deadline");
        }
        thread::sleep(Duration::from_millis(50));
    }

    let listing = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(root)
        .env("TITAN_HOME", titan_home)
        .args(["ls", "--json"])
        .output()
        .unwrap();
    let rows: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    assert_eq!(rows[0]["id"], run_id);
    assert_eq!(rows[0]["state"], "STOPPED");
}
