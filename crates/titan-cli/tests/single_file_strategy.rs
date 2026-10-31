//! User-facing single-file workflow and publication failure boundaries.
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn invoke(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(dir)
        .args(args)
        .env(
            "TITAN_STRATEGY_SDK",
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../python/titan-strategy-sdk"),
        )
        .output()
        .unwrap()
}
fn ok(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn source(dir: &Path, name: &str) {
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../../strategies/{name}.py")),
        dir.join(format!("{name}.py")),
    )
    .unwrap();
}

fn rehash_manifest(path: &Path, mut value: Value) {
    use sha2::{Digest, Sha256};
    value.as_object_mut().unwrap().remove("release_id");
    value["release_id"] = json!(format!(
        "{:x}",
        Sha256::digest(titan_strategy_runtime::compiler_json_v13(&value))
    ));
    fs::write(path.join("release.json"), value.to_string()).unwrap();
}

#[test]
fn defaults_discovery_native_scenes_and_legacy_flags_work_without_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    source(dir.path(), "event_counter");
    fs::write(
        dir.path().join("unimportable.py"),
        "raise RuntimeError('discovery must not import')",
    )
    .unwrap();
    let discovered = ok(invoke(dir.path(), &["strategy", "discover", ".", "--json"]));
    assert_eq!(discovered["sources"].as_array().unwrap().len(), 2);
    let compiled = ok(invoke(
        dir.path(),
        &["strategy", "compile", "event_counter.py", "--json"],
    ));
    assert_eq!(compiled["tests"]["passed"], 2);
    assert_eq!(compiled["paths"].as_array().unwrap().len(), 1);
    assert!(
        dir.path()
            .join("build/strategies/event_counter.titan")
            .is_file()
    );
    let human = invoke(dir.path(), &["strategy", "compile", "event_counter.py"]);
    assert!(human.status.success());
    let text = String::from_utf8(human.stdout).unwrap();
    assert!(
        text.len() < 1024
            && text.contains("native tests: 2/2 passed")
            && !text.contains("initial_state")
    );
    let tested = ok(invoke(
        dir.path(),
        &[
            "strategy",
            "test",
            "--strategy",
            "event_counter.py",
            "--json",
        ],
    ));
    assert_eq!(tested["tests"]["passed"], 2);
    assert_eq!(tested["valid"], true);
    let described = ok(invoke(
        dir.path(),
        &["strategy", "describe", "event_counter.py", "--json"],
    ));
    assert_eq!(described["normalized_parameters"], json!({}));
}

#[test]
fn parameter_overrides_are_typed_normalized_and_fail_before_replacing_output() {
    let dir = tempfile::tempdir().unwrap();
    source(dir.path(), "v13_smoke");
    let compiled = ok(invoke(
        dir.path(),
        &[
            "strategy",
            "compile",
            "v13_smoke.py",
            "--set",
            "initial_counter=7",
            "--json",
        ],
    ));
    assert_eq!(compiled["normalized_parameters"]["initial_counter"], 7);
    let output = dir.path().join("build/strategies/v13_smoke.titan");
    let before = fs::read(&output).unwrap();
    for value in [
        "initial_counter=true",
        "initial_counter=-1",
        "initial_counter=unknown",
        "absent=2",
    ] {
        let result = invoke(
            dir.path(),
            &[
                "strategy",
                "compile",
                "v13_smoke.py",
                "--set",
                value,
                "--json",
            ],
        );
        assert!(!result.status.success());
        assert_eq!(fs::read(&output).unwrap(), before);
    }
    fs::write(dir.path().join("override.json"), "{\"initial_counter\":1}").unwrap();
    let explicit = ok(invoke(
        dir.path(),
        &[
            "strategy",
            "compile",
            "--strategy",
            "v13_smoke.py",
            "--parameters",
            "override.json",
            "--set",
            "initial_counter=7",
            "--json",
        ],
    ));
    assert_eq!(
        compiled["compiled_strategy_digest"],
        explicit["compiled_strategy_digest"]
    );
    let path = dir.path().join("v13_smoke.py");
    fs::write(
        &path,
        fs::read_to_string(&path)
            .unwrap()
            .replace("\"window.2\": 3", "\"window.2\": 99"),
    )
    .unwrap();
    let failed = invoke(
        dir.path(),
        &[
            "strategy",
            "compile",
            "v13_smoke.py",
            "--set",
            "initial_counter=7",
            "--json",
        ],
    );
    assert!(!failed.status.success());
    assert_eq!(fs::read(&output).unwrap(), before);
}

#[test]
fn workspace_lock_rejects_drift_until_explicitly_updated() {
    let dir = tempfile::tempdir().unwrap();
    source(dir.path(), "event_counter");
    fs::write(
        dir.path().join("titan.toml"),
        "schema_version=1\n[strategy]\nbuild_dir='outputs'\n",
    )
    .unwrap();
    ok(invoke(
        dir.path(),
        &["strategy", "compile", "event_counter.py", "--json"],
    ));
    assert!(dir.path().join("outputs/event_counter.titan").exists());
    let path = dir.path().join("titan.lock");
    let mut lock: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    lock["sdk_digest"] = json!("wrong");
    fs::write(&path, lock.to_string()).unwrap();
    assert!(
        !invoke(
            dir.path(),
            &["strategy", "compile", "event_counter.py", "--json"]
        )
        .status
        .success()
    );
    ok(invoke(
        dir.path(),
        &[
            "strategy",
            "compile",
            "event_counter.py",
            "--update-lock",
            "--json",
        ],
    ));
    let restored: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_ne!(restored["sdk_digest"], "wrong");

    // A changed SDK must never execute, even if its import would have side effects.
    let sdk = dir.path().join("changed-sdk");
    fs::create_dir_all(sdk.join("titan_strategy")).unwrap();
    let marker = dir.path().join("sdk-executed");
    fs::write(
        sdk.join("titan_strategy/__init__.py"),
        format!(
            "from pathlib import Path\nPath({}).write_text('unexpected import')\n",
            serde_json::to_string(marker.to_str().unwrap()).unwrap()
        ),
    )
    .unwrap();
    for action in ["describe", "compile"] {
        let rejected = Command::new(env!("CARGO_BIN_EXE_titan"))
            .current_dir(dir.path())
            .env("TITAN_STRATEGY_SDK", &sdk)
            .args(["strategy", action, "event_counter.py", "--json"])
            .output()
            .unwrap();
        assert!(!rejected.status.success());
        assert!(String::from_utf8_lossy(&rejected.stderr).contains("before SDK execution"));
        assert!(!marker.exists());
    }
}

#[test]
fn runtime_build_defers_node_paths_without_weakening_runtime_validation() {
    let dir = tempfile::tempdir().unwrap();
    source(dir.path(), "event_counter");
    let mut runtime: toml::Value = toml::from_str(include_str!("fixtures/live.toml")).unwrap();
    runtime.as_table_mut().unwrap().insert(
        "account_service".into(),
        toml::Value::try_from(json!({"secret_root":"secrets"})).unwrap(),
    );
    runtime["strategy_service"].as_table_mut().unwrap().insert(
        "checkpoint_root".into(),
        toml::Value::String("state/checkpoints".into()),
    );
    let template = dir.path().join("runtime-template.toml");
    fs::write(&template, toml::to_string_pretty(&runtime).unwrap()).unwrap();
    let built = ok(invoke(
        dir.path(),
        &[
            "strategy",
            "build",
            "event_counter.py",
            "--runtime",
            template.to_str().unwrap(),
            "--json",
        ],
    ));
    let release = Path::new(built["release"].as_str().unwrap());
    let emitted: toml::Value =
        toml::from_str(&fs::read_to_string(release.join("runtime.toml")).unwrap()).unwrap();
    assert_eq!(
        emitted["account_service"]["secret_root"].as_str(),
        Some("secrets")
    );
    assert_eq!(
        emitted["strategy_service"]["checkpoint_root"].as_str(),
        Some("state/checkpoints")
    );
    assert!(!release.join("secrets").exists());
    assert!(!release.join("state").exists());
    ok(invoke(
        dir.path(),
        &[
            "strategy",
            "verify-release",
            release.to_str().unwrap(),
            "--json",
        ],
    ));
    // Node validation must still reject missing secrets instead of inheriting build-time placeholders.
    let rejected = invoke(
        dir.path(),
        &[
            "validate",
            "event_counter",
            "-e",
            "live",
            "-m",
            "tick",
            "-c",
            release.join("runtime.toml").to_str().unwrap(),
            "--json",
        ],
    );
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("secret root"));
    // Invalid runtime semantics must still prevent publication.
    runtime.as_table_mut().unwrap().insert(
        "execution".into(),
        toml::Value::try_from(json!({"worker_threads":0})).unwrap(),
    );
    fs::write(&template, toml::to_string_pretty(&runtime).unwrap()).unwrap();
    assert!(
        !invoke(
            dir.path(),
            &[
                "strategy",
                "build",
                "event_counter.py",
                "--runtime",
                template.to_str().unwrap(),
                "--json"
            ]
        )
        .status
        .success()
    );
    // Absolute deployment paths must not be created on the compiler either.
    runtime["execution"]["worker_threads"] = toml::Value::Integer(2);
    let node_only = dir.path().join("node-only");
    runtime["account_service"]["secret_root"] =
        toml::Value::String(node_only.join("secrets").to_string_lossy().into());
    runtime["strategy_service"]["checkpoint_root"] =
        toml::Value::String(node_only.join("checkpoints").to_string_lossy().into());
    fs::write(&template, toml::to_string_pretty(&runtime).unwrap()).unwrap();
    ok(invoke(
        dir.path(),
        &[
            "strategy",
            "build",
            "event_counter.py",
            "--runtime",
            template.to_str().unwrap(),
            "--json",
        ],
    ));
    assert!(!node_only.exists());
    runtime["strategy_service"]
        .as_table_mut()
        .unwrap()
        .remove("checkpoint_root");
    runtime["strategy_service"].as_table_mut().unwrap().insert(
        "recovery_storage".into(),
        toml::Value::try_from(
            json!({"driver":"sqlite","path":node_only.join("recovery/runtime.db")}),
        )
        .unwrap(),
    );
    fs::write(&template, toml::to_string_pretty(&runtime).unwrap()).unwrap();
    ok(invoke(
        dir.path(),
        &[
            "strategy",
            "build",
            "event_counter.py",
            "--runtime",
            template.to_str().unwrap(),
            "--json",
        ],
    ));
    assert!(!node_only.exists());
    runtime["strategy_service"]["recovery_storage"]["path"] =
        toml::Value::String(":memory:".into());
    fs::write(&template, toml::to_string_pretty(&runtime).unwrap()).unwrap();
    assert!(
        !invoke(
            dir.path(),
            &[
                "strategy",
                "build",
                "event_counter.py",
                "--runtime",
                template.to_str().unwrap(),
                "--json"
            ]
        )
        .status
        .success()
    );
}

#[test]
fn releases_publish_idempotently_and_reject_tampering_without_python() {
    let dir = tempfile::tempdir().unwrap();
    source(dir.path(), "event_counter");
    let built = ok(invoke(
        dir.path(),
        &["strategy", "build", "event_counter.py", "--json"],
    ));
    let release = built["release"].as_str().unwrap();
    let verified = Command::new(env!("CARGO_BIN_EXE_titan"))
        .current_dir(dir.path())
        .env("TITAN_STRATEGY_PYTHON", "/must-not-execute")
        .args(["strategy", "verify-release", release, "--json"])
        .output()
        .unwrap();
    assert_eq!(ok(verified)["release_id"], built["release_id"]);
    let destination = dir.path().join("published");
    for _ in 0..2 {
        let published = ok(invoke(
            dir.path(),
            &[
                "strategy",
                "publish",
                release,
                "--directory",
                destination.to_str().unwrap(),
                "--json",
            ],
        ));
        assert_eq!(published["release_id"], built["release_id"]);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let shim = dir.path().join("shim");
        fs::create_dir(&shim).unwrap();
        let ssh = shim.join("ssh");
        fs::write(
            &ssh,
            "#!/bin/bash\nfor arg in \"$@\"; do last=\"$arg\"; done\nexec sh -c \"$last\"\n",
        )
        .unwrap();
        fs::set_permissions(&ssh, fs::Permissions::from_mode(0o755)).unwrap();
        let destination = dir.path().join("node releases");
        let script =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/publish_strategy.sh");
        let result = Command::new("bash")
            .arg(script)
            .args([
                "--release",
                release,
                "--host",
                "offline-fixture",
                "--remote-root",
            ])
            .arg(&destination)
            .arg("--remote-titan")
            .arg(env!("CARGO_BIN_EXE_titan"))
            .env(
                "PATH",
                format!("{}:{}", shim.display(), std::env::var("PATH").unwrap()),
            )
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{} {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            destination
                .join(built["release_id"].as_str().unwrap())
                .join("release.json")
                .is_file()
        );
    }
    {
        use sha2::{Digest, Sha256};
        let path = Path::new(release);
        let original = fs::read(path.join("release.json")).unwrap();
        let manifest: Value = serde_json::from_slice(&original).unwrap();
        let mut changed = manifest.clone();
        changed["toolchain"]["titan_binary_sha256"] = json!("wrong-binary");
        rehash_manifest(path, changed);
        assert!(
            !invoke(
                dir.path(),
                &["strategy", "verify-release", release, "--json"]
            )
            .status
            .success()
        );
        let mut changed = manifest.clone();
        changed["files"].as_object_mut().unwrap().remove("titan");
        fs::rename(path.join("titan"), dir.path().join("saved-titan")).unwrap();
        rehash_manifest(path, changed);
        assert!(
            !invoke(
                dir.path(),
                &["strategy", "verify-release", release, "--json"]
            )
            .status
            .success()
        );
        fs::rename(dir.path().join("saved-titan"), path.join("titan")).unwrap();
        let verification_path = path.join("verification.json");
        let original_verification = fs::read(&verification_path).unwrap();
        let mut verification: Value = serde_json::from_slice(&original_verification).unwrap();
        verification["native_tests"]["passed"] = json!(3);
        let verification = verification.to_string();
        fs::write(&verification_path, &verification).unwrap();
        let mut changed = manifest;
        changed["files"]["verification.json"] =
            json!(format!("{:x}", Sha256::digest(verification.as_bytes())));
        rehash_manifest(path, changed);
        assert!(
            !invoke(
                dir.path(),
                &["strategy", "verify-release", release, "--json"]
            )
            .status
            .success()
        );
        fs::write(verification_path, original_verification).unwrap();
        fs::write(path.join("release.json"), original).unwrap();
    }
    fs::write(Path::new(release).join("source.py"), "tampered").unwrap();
    assert!(
        !invoke(
            dir.path(),
            &["strategy", "verify-release", release, "--json"]
        )
        .status
        .success()
    );
    assert!(
        !invoke(
            dir.path(),
            &[
                "strategy",
                "publish",
                release,
                "--directory",
                destination.to_str().unwrap(),
                "--json"
            ]
        )
        .status
        .success()
    );
}

#[test]
fn signed_release_requires_explicit_trust_and_test_requires_declared_scenes() {
    let dir = tempfile::tempdir().unwrap();
    source(dir.path(), "event_counter");
    fs::write(
        dir.path().join("key"),
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
    )
    .unwrap();
    let built = ok(invoke(
        dir.path(),
        &[
            "strategy",
            "build",
            "event_counter.py",
            "--signing-key",
            "key",
            "--key-id",
            "test",
            "--json",
        ],
    ));
    let release = built["release"].as_str().unwrap();
    assert!(
        !invoke(
            dir.path(),
            &[
                "strategy",
                "verify-release",
                release,
                "--require-signature",
                "--json"
            ]
        )
        .status
        .success()
    );
    let verified = ok(invoke(
        dir.path(),
        &[
            "strategy",
            "verify-release",
            release,
            "--require-signature",
            "--trusted-key",
            "test=d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
            "--json",
        ],
    ));
    assert_eq!(verified["valid"], true);
    // Rehashing an unsigned release index cannot forge source identity protected by the native signature.
    {
        use sha2::{Digest, Sha256};
        let manifest_path = Path::new(release).join("release.json");
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let forged = b"forged source";
        let source_hash = format!("{:x}", Sha256::digest(forged));
        fs::write(Path::new(release).join("source.py"), forged).unwrap();
        manifest["source_digest"] = json!(source_hash);
        manifest["files"]["source.py"] = json!(source_hash);
        manifest.as_object_mut().unwrap().remove("release_id");
        let id = format!(
            "{:x}",
            Sha256::digest(titan_strategy_runtime::compiler_json_v13(&manifest))
        );
        manifest["release_id"] = json!(id);
        fs::write(manifest_path, manifest.to_string()).unwrap();
        assert!(
            !invoke(
                dir.path(),
                &[
                    "strategy",
                    "verify-release",
                    release,
                    "--require-signature",
                    "--trusted-key",
                    "test=d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
                    "--json"
                ]
            )
            .status
            .success()
        );
    }
    let path = dir.path().join("event_counter.py");
    let text = fs::read_to_string(&path).unwrap();
    fs::write(&path, text.split("TESTS =").next().unwrap()).unwrap();
    assert!(
        !invoke(
            dir.path(),
            &["strategy", "test", "event_counter.py", "--json"]
        )
        .status
        .success()
    );
    let result = ok(invoke(
        dir.path(),
        &["strategy", "compile", "event_counter.py", "--json"],
    ));
    assert_eq!(result["tests"]["mode"], "not_declared");
}
