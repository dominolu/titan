//! `titan instruments verify` is the offline, read-only entry point for instrument units.
//!
//! These tests pin the contract the Python preflight depends on: a stable exit code, a JSON report
//! with reason codes, and refusal of configurations whose units cannot be proven consistent.

use std::{fs, path::PathBuf, process::Command};

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn run(config: &str) -> std::process::Output {
    let directory = std::env::temp_dir().join(format!(
        "titan-instruments-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("runtime.toml");
    fs::write(&path, config).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args([
            "instruments",
            "verify",
            "--config",
            path.to_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    let _ = fs::remove_dir_all(&directory);
    output
}

const CONSISTENT: &str = r#"
schema_version = 1

[[market_sources]]
source_key = "maker"
connector_type = "okx"
connector_config = ""
instruments = [{ native_symbol = "BTC-USDT-SWAP", asset_id = 1001, price_tick = { coefficient = 1, scale = 1 }, quantity_lot = { coefficient = 1, scale = 2 }, contract_multiplier = { coefficient = 1, scale = 0 }, linear_quote_currency = "USDT" }]
enabled = true
definition_version = 1

[[accounts]]
account_key = "maker"
account_id = 2001
connector_type = "okx-account"
credential_ref = "secret://file/maker.toml"
connector_config = ""
instruments = [{ native_symbol = "BTC-USDT-SWAP", asset_id = 1001, price_tick = { coefficient = 1, scale = 1 }, quantity_lot = { coefficient = 1, scale = 2 }, contract_multiplier = { coefficient = 1, scale = 0 } }]
currencies = [{ native_currency = "USDT", currency_id = 1, amount_unit = { coefficient = 1, scale = 8 } }]
ownership = "ObserveAll"
shutdown_order_policy = "CancelAll"
enabled = true
definition_version = 1
"#;

#[test]
fn consistent_units_pass_and_report_derived_base_delta() {
    let output = run(CONSISTENT);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["valid"], serde_json::json!(true));
    assert_eq!(report["errors"], serde_json::json!([]));
    let instrument = &report["instruments"][0];
    assert_eq!(instrument["asset_id"], serde_json::json!(1001));
    assert_eq!(instrument["base_delta_per_lot"], serde_json::json!("0.01"));
    assert_eq!(instrument["min_qty_lots"], serde_json::Value::Null);
    assert_eq!(instrument["quote_currency"], serde_json::json!("USDT"));
}

#[test]
fn market_and_account_multiplier_mismatch_is_refused() {
    // The account-side binding is the only one without a quote currency, so this key is unique.
    let config = CONSISTENT.replace(
        "contract_multiplier = { coefficient = 1, scale = 0 } }]",
        "contract_multiplier = { coefficient = 2, scale = 0 } }]",
    );
    assert_ne!(
        config, CONSISTENT,
        "test fixture replacement must take effect"
    );
    let output = run(&config);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["valid"], serde_json::json!(false));
    assert_eq!(
        report["errors"][0]["code"],
        serde_json::json!("instrument_units_market_account_mismatch")
    );
    assert!(
        report["errors"][0]["detail"]
            .as_str()
            .unwrap()
            .contains("contract_multiplier")
    );
}

#[test]
fn unknown_binding_field_is_a_configuration_error_not_a_silent_ignore() {
    // The historical bug: `contract_multiplier` in a market source was silently dropped. A typo
    // must now fail the parse instead of producing a different running configuration.
    let config = CONSISTENT.replace(
        "linear_quote_currency = \"USDT\" }]",
        "linear_quote_currency = \"USDT\", typo_field = 1 }]",
    );
    assert_ne!(
        config, CONSISTENT,
        "test fixture replacement must take effect"
    );
    let output = run(&config);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["valid"], serde_json::json!(false));
    assert_eq!(
        report["errors"][0]["code"],
        serde_json::json!("configuration_invalid")
    );
}

#[test]
fn missing_contract_multiplier_is_refused() {
    let config = CONSISTENT.replace(
        "contract_multiplier = { coefficient = 1, scale = 0 }, linear_quote_currency",
        "linear_quote_currency",
    );
    assert_ne!(
        config, CONSISTENT,
        "test fixture replacement must take effect"
    );
    let output = run(&config);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        report["errors"][0]["code"],
        serde_json::json!("configuration_invalid")
    );
}

#[test]
fn invalid_decimal_unit_is_refused() {
    let config = CONSISTENT.replacen(
        "price_tick = { coefficient = 1, scale = 1 }",
        "price_tick = { coefficient = 0, scale = 1 }",
        1,
    );
    assert_ne!(
        config, CONSISTENT,
        "test fixture replacement must take effect"
    );
    let output = run(&config);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        report["errors"][0]["code"],
        serde_json::json!("configuration_invalid")
    );
}

fn write_dir(files: &[(&str, &str)]) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "titan-instruments-meta-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).unwrap();
    for (name, contents) in files {
        fs::write(directory.join(name), contents).unwrap();
    }
    directory
}

fn invoke(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(workspace())
        .args(args)
        .output()
        .unwrap()
}

fn snapshot(multiplier_scale: &str) -> String {
    format!(
        r#"{{
  "schema_version": 1,
  "captured_at_ns": 1700000000000,
  "instruments": [
    {{
      "asset_no": 1001,
      "native_symbol": "BTC-USDT-SWAP",
      "source_key": "maker",
      "price_tick": {{ "coefficient": 1, "scale": 1 }},
      "quantity_lot": {{ "coefficient": 1, "scale": 2 }},
      "contract_multiplier": {{ "coefficient": {multiplier_scale}, "scale": 0 }},
      "linear_quote_currency": "USDT",
      "synthetic": false
    }}
  ],
  "digest": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
}}
"#
    )
}

#[test]
fn metadata_snapshot_proves_units_and_refuses_contradictions() {
    let directory = write_dir(&[
        ("runtime.toml", CONSISTENT),
        ("snapshot.json", &snapshot("1")),
        ("wrong.json", &snapshot("2")),
    ]);
    let config = directory.join("runtime.toml");
    let good = directory.join("snapshot.json");
    let wrong = directory.join("wrong.json");

    // The stored digest is all zeros: the snapshot is refused before any comparison.
    let output = invoke(&[
        "instruments",
        "verify",
        "--config",
        config.to_str().unwrap(),
        "--metadata-snapshot",
        good.to_str().unwrap(),
        "--json",
    ]);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        report["errors"][0]["code"],
        serde_json::json!("instrument_metadata_digest_mismatch")
    );
    assert_eq!(report["metadata_verified"], serde_json::json!(true));

    // Refreshing the digest makes the same bytes authoritative.
    for file in [&good, &wrong] {
        let output = invoke(&[
            "instruments",
            "snapshot-digest",
            file.to_str().unwrap(),
            "--update",
        ]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let output = invoke(&[
        "instruments",
        "verify",
        "--config",
        config.to_str().unwrap(),
        "--metadata-snapshot",
        good.to_str().unwrap(),
        "--json",
    ]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["valid"], serde_json::json!(true));
    assert_eq!(report["metadata_verified"], serde_json::json!(true));
    assert!(report["metadata_digest"].as_str().unwrap().len() == 64);

    // The configuration claims one base delta while the metadata publishes another.
    let output = invoke(&[
        "instruments",
        "verify",
        "--config",
        config.to_str().unwrap(),
        "--metadata-snapshot",
        wrong.to_str().unwrap(),
        "--json",
    ]);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        report["errors"][0]["code"],
        serde_json::json!("instrument_metadata_mismatch")
    );
    assert!(
        report["errors"][0]["detail"]
            .as_str()
            .unwrap()
            .contains("contract_multiplier")
    );

    let _ = fs::remove_dir_all(&directory);
}
