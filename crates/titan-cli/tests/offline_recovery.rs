//! Offline recovery must follow the online restore contract, not a simplified variant.
//!
//! `OfflineAdapter::recover` restores a verified snapshot, lets the strategy reconcile against
//! the same recovery-order payload the live host exposes, and refuses a recovery callback that
//! tries to issue commands. These tests drive the real CLI compiler and the real native callback
//! boundary; they need the configured Python strategy environment, like the other single-file
//! workflow tests.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex, OnceLock},
};

use titan_strategy_runtime::{
    NativeStrategyLoader, OfflineAdapter, OfflineConfig, StrategyStateSnapshot,
};

fn workspace() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Strategy compilation spawns Python/Numba and is CPU-heavy. Serializing compilations inside this
/// test binary keeps the product's 60 s `describe` limit meaningful on a shared build server; a
/// parallel suite otherwise makes three native compiles compete for the same cores.
fn compile_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn compile(directory: &PathBuf, source: &str, extra: &[&str], output: &str) -> PathBuf {
    let _guard = compile_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let output = directory.join(output);
    let mut command = Command::new(env!("CARGO_BIN_EXE_titan"));
    command
        // Compile inside the temporary workspace so `titan.lock` never lands in the repository.
        .current_dir(directory)
        .env(
            "TITAN_STRATEGY_SDK",
            workspace().join("python/titan-strategy-sdk"),
        )
        .arg("strategy")
        .arg("compile")
        .arg("--strategy")
        .arg(workspace().join(source))
        .arg("--artifact-format")
        .arg("bundle")
        .arg("--output")
        .arg(&output)
        .args(extra);
    // The build server is shared; a native describe can exceed the product's 60 s limit purely
    // because of external CPU contention. Retry that specific environmental failure instead of
    // making the recovery contract look broken.
    for attempt in 0..3 {
        let result = command.output().unwrap();
        if result.status.success() {
            return output;
        }
        let stderr = String::from_utf8_lossy(&result.stderr);
        let contended = stderr.contains("exceeded its 60 second limit");
        assert!(
            contended && attempt < 2,
            "compile failed: {}\n{}",
            String::from_utf8_lossy(&result.stdout),
            stderr
        );
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
    unreachable!("compile retries return or fail above")
}

fn load(bundle: &Path, cache: &Path) -> Arc<titan_strategy_runtime::LoadedNativeStrategy> {
    Arc::new(
        NativeStrategyLoader::new(cache.to_path_buf())
            .load(bundle)
            .expect("load compiled strategy"),
    )
}

fn adapter(
    compiled: &Arc<titan_strategy_runtime::LoadedNativeStrategy>,
    instance: u64,
) -> OfflineAdapter {
    OfflineAdapter::new(OfflineConfig {
        compiled_strategy: compiled.clone(),
        strategy_instance_id: instance,
        generation: 1,
        command_capacity: 8,
        markets: Vec::new(),
        positions: Vec::new(),
        balances: Vec::new(),
        accounts: Vec::new(),
        active_orders: Vec::new(),
        object_table: None,
    })
    .expect("create offline adapter")
}

fn counter(snapshot: &StrategyStateSnapshot) -> i64 {
    i64::from_le_bytes(snapshot.state_bytes[..8].try_into().unwrap())
}

#[test]
fn offline_recovery_restores_state_and_completes_the_reconcile_boundary() {
    let directory =
        std::env::temp_dir().join(format!("titan-offline-recover-{}", std::process::id()));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).unwrap();
    let bundle = compile(
        &directory,
        "python/titan-strategy-sdk/tests/fixtures/timer_submit.py",
        &["--set", "initial_counter=4"],
        "timer.titan",
    );
    let compiled = load(&bundle, &directory);

    // Freeze a checkpoint from a running instance, as the live host does.
    let mut running = adapter(&compiled, 7);
    running.start(1).expect("start");
    let snapshot = running.freeze_checkpoint(1).expect("freeze");
    assert_eq!(counter(&snapshot), 4);

    // A fresh adapter restores before start: same instance identity, next generation.
    let mut recovered = adapter(&compiled, 7);
    recovered
        .recover(2, &snapshot, &[], 2)
        .expect("offline recovery succeeds");
    assert_eq!(counter(&recovered.freeze_checkpoint(2).unwrap()), 4);
    assert_eq!(
        recovered.freeze_checkpoint(3).unwrap().generation,
        2,
        "recovery advances the generation"
    );

    // Restoring a running instance is refused: the online host only restores at a safe point.
    let mut not_stopped = adapter(&compiled, 7);
    not_stopped.start(1).unwrap();
    assert!(not_stopped.recover(2, &snapshot, &[], 2).is_err());

    // A snapshot from another instance identity must not be accepted.
    let mut foreign = adapter(&compiled, 8);
    assert!(foreign.recover(2, &snapshot, &[], 2).is_err());

    let _ = fs::remove_dir_all(&directory);
}

#[test]
fn recovery_orders_are_injected_without_breaking_strategies_that_do_not_declare_them() {
    let directory = std::env::temp_dir().join(format!(
        "titan-offline-recover-orders-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).unwrap();
    let bundle = compile(
        &directory,
        "python/titan-strategy-sdk/tests/fixtures/timer_submit.py",
        &["--set", "initial_counter=1"],
        "timer.titan",
    );
    let compiled = load(&bundle, &directory);

    let mut running = adapter(&compiled, 3);
    running.start(1).unwrap();
    let snapshot = running.freeze_checkpoint(1).unwrap();

    let order = titan_strategy_runtime::TitanActiveOrderView {
        order_id: 11,
        asset_no: 0,
        account_no: 0,
        status: 1,
        ..Default::default()
    };
    let mut recovered = adapter(&compiled, 3);
    recovered
        .recover(2, &snapshot, &[order], 2)
        .expect("an undeclared payload is simply not exposed");

    let _ = fs::remove_dir_all(&directory);
}

#[test]
fn recovery_callback_must_not_issue_commands() {
    let directory = std::env::temp_dir().join(format!(
        "titan-offline-recover-submit-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).unwrap();
    let bundle = compile(
        &directory,
        "python/titan-strategy-sdk/tests/fixtures/recover_submit.py",
        &[],
        "recover_submit.titan",
    );
    let compiled = load(&bundle, &directory);

    let mut running = adapter(&compiled, 5);
    running.start(1).unwrap();
    let snapshot = running.freeze_checkpoint(1).unwrap();

    let mut recovered = adapter(&compiled, 5);
    let error = recovered
        .recover(2, &snapshot, &[], 2)
        .expect_err("a recovery command must fail closed");
    assert!(
        error.to_string().contains("recovery_callback"),
        "the recovery callback must fail closed: {error}"
    );

    let _ = fs::remove_dir_all(&directory);
}
