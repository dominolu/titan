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

fn run(strategy: &str, mode: &str, config: &str) -> serde_json::Value {
    let root = workspace();
    let titan_home = std::env::temp_dir().join(format!(
        "titan-cli-test-{}-{}",
        std::process::id(),
        config.replace('.', "-")
    ));
    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", titan_home)
        .args([
            "run",
            strategy,
            "-e",
            "backtest",
            "-m",
            mode,
            "-c",
            &format!("crates/titan-cli/tests/fixtures/{config}"),
        ])
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
    let bar = run("dual_ma", "bar", "dual_ma.toml");
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

    let tick = run("event_counter", "tick", "tick.toml");
    assert_eq!(tick["market_event_count"], 12);
    assert_eq!(tick["callback_count"][6], 6);
    assert_eq!(tick["state_f64"][0], 12.0);

    let hybrid = run("event_counter", "hybrid", "hybrid.toml");
    assert_eq!(hybrid["market_event_count"], 17);
    assert_eq!(hybrid["callback_count"][5], 5);
    assert_eq!(hybrid["state_f64"][0], 12.0);
    assert_eq!(hybrid["state_f64"][2], 5.0);
}

#[test]
fn public_cli_resolves_toml_to_an_internal_run_spec_and_exposes_agent_json() {
    let root = workspace();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let titan_home =
        std::env::temp_dir().join(format!("titan-cli-agent-{}-{nonce}", std::process::id()));
    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args([
            "run",
            "dual_ma",
            "-e",
            "backtest",
            "-m",
            "bar",
            "-c",
            "crates/titan-cli/tests/fixtures/dual_ma.toml",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let run_id = result["run_id"].as_str().unwrap();
    assert_eq!(result["environment"], "backtest");
    assert_eq!(result["event_mode"], "bar");

    let listing = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args([
            "ls",
            "--json",
            "--env",
            "backtest",
            "--mode",
            "bar",
            "--strategy",
            "dual_ma",
            "--status",
            "completed",
        ])
        .output()
        .unwrap();
    assert!(listing.status.success());
    let rows: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    assert_eq!(rows["schema_version"], 1);
    assert_eq!(rows["runs"].as_array().unwrap().len(), 1);
    assert_eq!(rows["runs"][0]["id"], run_id);
    assert_eq!(rows["runs"][0]["strategy_id"], "dual_ma");
    assert_eq!(rows["runs"][0]["health"], "HEALTHY");
    assert_eq!(rows["runs"][0]["market_event_count"], 5);

    let spec_path = PathBuf::from(rows["runs"][0]["spec_path"].as_str().unwrap());
    let spec: serde_json::Value =
        serde_json::from_slice(&std::fs::read(spec_path).unwrap()).unwrap();
    assert_eq!(spec["environment"], "backtest");
    assert_eq!(spec["event_mode"], "bar");
    assert_eq!(spec["backend"]["kind"], "backtest");
    assert_eq!(spec["backend"]["source"]["kind"], "bar");
    assert_eq!(spec["backend"]["execution"]["exchange"], "no_partial_fill");
    assert_eq!(spec["backend"]["execution"]["queue"], "power_probability");

    let active = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(root)
        .env("TITAN_HOME", titan_home)
        .args(["ls", "--json", "--active"])
        .output()
        .unwrap();
    let active: serde_json::Value = serde_json::from_slice(&active.stdout).unwrap();
    assert!(active["runs"].as_array().unwrap().is_empty());
}

#[test]
fn detached_run_returns_agent_json_and_reaches_completed() {
    let root = workspace();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let titan_home =
        std::env::temp_dir().join(format!("titan-cli-detach-{}-{nonce}", std::process::id()));
    let started = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args([
            "run",
            "dual_ma",
            "-e",
            "backtest",
            "-m",
            "bar",
            "-c",
            "crates/titan-cli/tests/fixtures/dual_ma.toml",
            "--detach",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(started.status.success());
    let started: serde_json::Value = serde_json::from_slice(&started.stdout).unwrap();
    let run_id = started["run_id"].as_str().unwrap();
    assert_eq!(started["state"], "STARTING");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let shown = Command::new(env!("CARGO_BIN_EXE_titan"))
            .current_dir(&root)
            .env("TITAN_HOME", &titan_home)
            .args(["show", run_id, "--json"])
            .output()
            .unwrap();
        let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
        match shown["run"]["state"].as_str().unwrap() {
            "COMPLETED" => {
                assert!(shown["run"]["pid"].is_null());
                assert_eq!(shown["run"]["market_event_count"], 5);
                break;
            }
            "FAILED" | "STALE" => panic!("detached worker failed: {shown}"),
            _ => {}
        }
        assert!(
            Instant::now() < deadline,
            "detached worker did not complete"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn invalid_mode_is_rejected_as_stable_json_before_python_starts() {
    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args([
            "validate",
            "dual_ma",
            "-e",
            "backtest",
            "-m",
            "hybrid",
            "-c",
            "crates/titan-cli/tests/fixtures/hybrid.toml",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(10));
    let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "INVALID_CONFIGURATION");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not support event mode")
    );
    assert!(
        !error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Tick/Hybrid engine")
    );

    let usage = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args(["run", "dual_ma", "--json"])
        .output()
        .unwrap();
    assert_eq!(usage.status.code(), Some(2));
    let usage: serde_json::Value = serde_json::from_slice(&usage.stderr).unwrap();
    assert_eq!(usage["error"]["code"], "CLI_USAGE");

    let missing = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args(["strategy", "show", "missing_strategy", "--json"])
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(41));
    let missing: serde_json::Value = serde_json::from_slice(&missing.stderr).unwrap();
    assert_eq!(missing["error"]["code"], "STRATEGY_NOT_FOUND");
}

#[test]
fn strategy_catalog_is_static_and_machine_readable() {
    let listed = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args(["strategy", "ls", "--json"])
        .output()
        .unwrap();
    assert!(listed.status.success());
    let catalog: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    let dual_ma = catalog["strategies"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["strategy_id"] == "dual_ma")
        .unwrap();
    assert_eq!(dual_ma["status"], "VALID");
    assert_eq!(dual_ma["events"], serde_json::json!(["bar", "tick"]));
    assert_eq!(
        dual_ma["environments"],
        serde_json::json!(["backtest", "live"])
    );

    let validated = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args(["strategy", "validate", "dual_ma", "--json"])
        .output()
        .unwrap();
    assert!(validated.status.success());
    let validated: serde_json::Value = serde_json::from_slice(&validated.stdout).unwrap();
    assert_eq!(validated["valid"], true);
}

#[test]
fn python_stdout_never_contaminates_agent_json() {
    let root = workspace();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let titan_home =
        std::env::temp_dir().join(format!("titan-cli-noisy-{}-{nonce}", std::process::id()));
    let strategies = root.join("crates/titan-cli/tests/fixtures/strategies");

    let compiled = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .env("TITAN_STRATEGIES", &strategies)
        .args(["strategy", "compile", "noisy", "--json"])
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let compiled: serde_json::Value = serde_json::from_slice(&compiled.stdout).unwrap();
    assert_eq!(compiled["strategy_id"], "noisy");

    let run = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .env("TITAN_STRATEGIES", strategies)
        .args([
            "run",
            "noisy",
            "-e",
            "backtest",
            "-m",
            "bar",
            "-c",
            "crates/titan-cli/tests/fixtures/noisy.toml",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&run.stdout).unwrap();
    assert_eq!(result["strategy_id"], "noisy");
    assert_eq!(result["market_event_count"], 5);
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
        .env(
            "TITAN_STRATEGIES",
            root.join("crates/titan-cli/tests/fixtures/strategies"),
        )
        .args([
            "run",
            "compile_failure",
            "-e",
            "backtest",
            "-m",
            "bar",
            "-c",
            "crates/titan-cli/tests/fixtures/compile_failure.toml",
            "--json",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(31));
    let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "WORKER_FAILED");
    let listing = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(root)
        .env("TITAN_HOME", titan_home)
        .args(["ls", "--json"])
        .output()
        .unwrap();
    let runs: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    assert_eq!(runs["runs"][0]["state"], "FAILED");
    assert!(
        runs["runs"][0]["error"]
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
        .args([
            "run",
            "dual_ma",
            "-e",
            "backtest",
            "-m",
            "bar",
            "-c",
            "crates/titan-cli/tests/fixtures/dual_ma.toml",
        ])
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
    let run_id = rows["runs"][0]["id"].as_str().unwrap();
    let report = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args(["report", run_id])
        .output()
        .unwrap();
    assert!(report.status.success());
    let result: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert!(result["execution_reports"].is_array());

    let python = std::env::var("PYO3_PYTHON").unwrap_or_else(|_| "python3".into());
    let quantstats_path = titan_home.join("quantstats-report.html");
    let quantstats_report = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .env("TITAN_REPORT_PYTHON", &python)
        .env("TITAN_REPORTING_PATH", root.join("python/titan-reporting"))
        .args([
            "report",
            run_id,
            "--renderer",
            "quantstats",
            "--output",
            quantstats_path.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        quantstats_report.status.success(),
        "{}",
        String::from_utf8_lossy(&quantstats_report.stderr)
    );
    let quantstats: serde_json::Value = serde_json::from_slice(&quantstats_report.stdout).unwrap();
    assert_eq!(quantstats["renderer"], "quantstats");
    assert!(
        std::fs::read_to_string(&quantstats_path)
            .unwrap()
            .contains("No canonical return observations")
    );
    let quantstats_state = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args(["show", run_id, "--json"])
        .output()
        .unwrap();
    let quantstats_state: serde_json::Value =
        serde_json::from_slice(&quantstats_state.stdout).unwrap();
    assert_eq!(quantstats_state["run"]["state"], "COMPLETED");
    assert_eq!(quantstats_state["run"]["report_state"], "READY");

    let report_path = titan_home.join("native-report.html");
    let rendered = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .env("TITAN_REPORT_PYTHON", python)
        .env("TITAN_REPORTING_PATH", root.join("python/titan-reporting"))
        .args([
            "report",
            run_id,
            "--renderer",
            "native",
            "--output",
            report_path.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        rendered.status.success(),
        "{}",
        String::from_utf8_lossy(&rendered.stderr)
    );
    let rendered: serde_json::Value = serde_json::from_slice(&rendered.stdout).unwrap();
    assert_eq!(rendered["report_path"], report_path.to_str().unwrap());
    assert!(report_path.is_file());

    let shown = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args(["show", run_id, "--json"])
        .output()
        .unwrap();
    let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(shown["run"]["report_state"], "READY");
    assert_eq!(shown["run"]["report_path"], report_path.to_str().unwrap());

    let result_path = PathBuf::from(rows["runs"][0]["result_path"].as_str().unwrap());
    let original_result = std::fs::read(&result_path).unwrap();
    let protected = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .env("TITAN_REPORTING_PATH", root.join("python/titan-reporting"))
        .args([
            "report",
            run_id,
            "--output",
            result_path.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!protected.status.success());
    assert!(String::from_utf8_lossy(&protected.stderr).contains("immutable ResultBundle"));
    assert_eq!(std::fs::read(&result_path).unwrap(), original_result);

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
            "event_counter",
            "-e",
            "live",
            "-m",
            "tick",
            "-c",
            "crates/titan-cli/tests/fixtures/live.toml",
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
fn detached_worker_has_an_independent_session_and_show_reconciles_a_crash() {
    let root = workspace();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let titan_home =
        std::env::temp_dir().join(format!("titan-cli-stale-{}-{nonce}", std::process::id()));
    let started = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args([
            "run",
            "event_counter",
            "-e",
            "live",
            "-m",
            "tick",
            "-c",
            "crates/titan-cli/tests/fixtures/live.toml",
            "--detach",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    let started: serde_json::Value = serde_json::from_slice(&started.stdout).unwrap();
    let run_id = started["run_id"].as_str().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let pid = loop {
        let shown = Command::new(env!("CARGO_BIN_EXE_titan"))
            .current_dir(&root)
            .env("TITAN_HOME", &titan_home)
            .args(["show", run_id, "--json"])
            .output()
            .unwrap();
        let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
        if shown["run"]["state"] == "RUNNING" {
            break shown["run"]["pid"].as_u64().unwrap() as i32;
        }
        assert!(Instant::now() < deadline, "worker did not enter RUNNING");
        thread::sleep(Duration::from_millis(50));
    };

    // Safety: pid belongs to the exact detached worker created above.
    assert_ne!(unsafe { libc::getsid(pid) }, unsafe { libc::getsid(0) });
    // Safety: pid belongs to the exact detached worker created above.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    loop {
        let shown = Command::new(env!("CARGO_BIN_EXE_titan"))
            .current_dir(&root)
            .env("TITAN_HOME", &titan_home)
            .args(["show", run_id, "--json"])
            .output()
            .unwrap();
        let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
        if shown["run"]["state"] == "STALE" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "show did not reconcile dead worker"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
#[test]
fn text_stop_confirms_the_requested_transition() {
    let root = workspace();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let titan_home = std::env::temp_dir().join(format!(
        "titan-cli-stop-text-{}-{nonce}",
        std::process::id()
    ));
    let started = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args([
            "run",
            "event_counter",
            "-e",
            "live",
            "-m",
            "tick",
            "-c",
            "crates/titan-cli/tests/fixtures/live.toml",
            "--detach",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(started.status.success());
    let started: serde_json::Value = serde_json::from_slice(&started.stdout).unwrap();
    let run_id = started["run_id"].as_str().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let shown = Command::new(env!("CARGO_BIN_EXE_titan"))
            .current_dir(&root)
            .env("TITAN_HOME", &titan_home)
            .args(["show", run_id, "--json"])
            .output()
            .unwrap();
        let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
        if shown["run"]["state"] == "RUNNING" {
            break;
        }
        assert!(Instant::now() < deadline, "worker did not enter RUNNING");
        thread::sleep(Duration::from_millis(50));
    }

    let stopped = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(&root)
        .env("TITAN_HOME", &titan_home)
        .args(["stop", run_id])
        .output()
        .unwrap();
    assert!(stopped.status.success());
    assert_eq!(stopped.stdout, b"STOP_REQUESTED\n");
    loop {
        let shown = Command::new(env!("CARGO_BIN_EXE_titan"))
            .current_dir(&root)
            .env("TITAN_HOME", &titan_home)
            .args(["show", run_id, "--json"])
            .output()
            .unwrap();
        let shown: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
        if shown["run"]["state"] == "STOPPED" {
            break;
        }
        assert!(Instant::now() < deadline, "worker did not stop");
        thread::sleep(Duration::from_millis(50));
    }
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
        .args([
            "run",
            "event_counter",
            "-e",
            "live",
            "-m",
            "tick",
            "-c",
            "crates/titan-cli/tests/fixtures/live.toml",
        ])
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
            if let Some(run) = rows["runs"].as_array().and_then(|rows| rows.first())
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
    assert_eq!(rows["runs"][0]["id"], run_id);
    assert_eq!(rows["runs"][0]["state"], "STOPPED");
}
