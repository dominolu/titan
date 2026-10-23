//! Orchestrates disposable Numba workers; all platform and artifact policy stays in Rust.
use super::{CliError, CompileV13Request, hex_digest};
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use titan_strategy_runtime::{
    ArtifactBuildV13, NumbaMetadataV13, V13_ABI_FINGERPRINT, compiler_host_target_v13,
    compiler_json_v13, validate_compile_target_v13, write_artifact_v13,
};

fn failed(error: impl std::fmt::Display) -> CliError {
    CliError::StaticCompile(error.to_string())
}

struct RunningTool(Option<Child>);
impl Drop for RunningTool {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            #[cfg(unix)]
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGKILL);
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn run_tool(
    command: &mut Command,
    work: &Path,
    label: &str,
    timeout: Duration,
    stop: &AtomicBool,
) -> Result<String, CliError> {
    if stop.load(Ordering::Acquire) {
        return Err(failed("compilation interrupted"));
    }
    let stdout = work.join(format!("{label}.stdout"));
    let stderr = work.join(format!("{label}.stderr"));
    command
        .stdin(Stdio::null())
        .stdout(File::create(&stdout).map_err(failed)?)
        .stderr(File::create(&stderr).map_err(failed)?);
    #[cfg(unix)]
    {
        command.process_group(0);
        let cpu_seconds = if label == "describe" {
            30
        } else {
            timeout.as_secs().max(1)
        };
        // Restore the resource bounds previously owned by the Python loader process.
        unsafe {
            command.pre_exec(move || {
                let memory = libc::rlimit {
                    rlim_cur: 4 * 1024 * 1024 * 1024,
                    rlim_max: 4 * 1024 * 1024 * 1024,
                };
                let cpu = libc::rlimit {
                    rlim_cur: cpu_seconds,
                    rlim_max: cpu_seconds,
                };
                if libc::setrlimit(libc::RLIMIT_AS, &memory) != 0
                    || libc::setrlimit(libc::RLIMIT_CPU, &cpu) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let mut running = RunningTool(Some(command.spawn().map_err(failed)?));
    let deadline = Instant::now() + timeout;
    loop {
        if stop.load(Ordering::Acquire) {
            return Err(failed("compilation interrupted"));
        }
        let status = running.0.as_mut().unwrap().try_wait().map_err(failed)?;
        if let Some(status) = status {
            // No further child is spawned by our Python bridge after it finishes.
            running.0 = None;
            if !status.success() {
                let mut details = String::new();
                File::open(&stderr)
                    .map_err(failed)?
                    .take(65_536)
                    .read_to_string(&mut details)
                    .map_err(failed)?;
                return Err(failed(format!("{label} failed ({status}): {details}")));
            }
            let file = File::open(&stdout).map_err(failed)?;
            if file.metadata().map_err(failed)?.len() > 4 * 1024 * 1024 {
                return Err(failed("tool output exceeds compiler limit"));
            }
            return fs::read_to_string(stdout).map_err(failed);
        }
        if Instant::now() >= deadline {
            return Err(failed(format!(
                "{label} exceeded its {} second limit",
                timeout.as_secs()
            )));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn read_metadata(path: &Path) -> Result<NumbaMetadataV13, CliError> {
    if fs::metadata(path).map_err(failed)?.len() > 4 * 1024 * 1024 {
        return Err(failed("Numba metadata exceeds compiler limit"));
    }
    let metadata: NumbaMetadataV13 =
        serde_json::from_slice(&fs::read(path).map_err(failed)?).map_err(failed)?;
    metadata.validate().map_err(failed)?;
    Ok(metadata)
}

fn read_seed(path: &Path) -> Result<[u8; 32], CliError> {
    let raw = fs::read(path).map_err(failed)?;
    // Raw seeds are binary: never trim whitespace bytes from a 32-byte seed.
    if raw.len() == 32 {
        return Ok(raw.try_into().unwrap());
    }
    let encoded = std::str::from_utf8(&raw)
        .map_err(|_| failed("signing key must be 32 raw bytes or 64 hex characters"))?
        .trim();
    if encoded.len() != 64 {
        return Err(failed(
            "signing key must be 32 raw bytes or 64 hex characters",
        ));
    }
    let mut seed = [0; 32];
    for (index, byte) in seed.iter_mut().enumerate() {
        let pair = encoded.as_bytes().get(index * 2..index * 2 + 2).unwrap();
        let pair = std::str::from_utf8(pair).map_err(|_| failed("signing key is not ASCII hex"))?;
        *byte = u8::from_str_radix(pair, 16).map_err(|_| failed("signing key is not ASCII hex"))?;
    }
    Ok(seed)
}

fn forbidden_symbol(name: &str) -> bool {
    let name = name.trim_start_matches('_');
    name.starts_with("Py") || name.starts_with("numba_") || name.starts_with("NRT_")
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NumbaObject {
    callback: String,
    implementation: String,
}

fn normalize_objects(
    work: &Path,
    metadata: &NumbaMetadataV13,
    stop: &AtomicBool,
) -> Result<(), CliError> {
    let path = work.join("objects.json");
    if fs::metadata(&path).map_err(failed)?.len() > 65_536 {
        return Err(failed("Numba object report exceeds compiler limit"));
    }
    let objects: Vec<NumbaObject> =
        serde_json::from_slice(&fs::read(path).map_err(failed)?).map_err(failed)?;
    let names: BTreeSet<_> = objects
        .iter()
        .map(|object| object.callback.as_str())
        .collect();
    let expected: BTreeSet<_> = metadata.callbacks.iter().map(String::as_str).collect();
    if names != expected || objects.len() != names.len() {
        return Err(failed("Numba object set does not match callbacks"));
    }
    for object in objects {
        if object.implementation.is_empty()
            || object.implementation.len() > 4096
            || !object
                .implementation
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_.$".contains(&byte))
        {
            return Err(failed("invalid Numba implementation symbol"));
        }
        let path = work.join(format!("{}.o", object.callback));
        if !fs::symlink_metadata(&path)
            .map_err(failed)?
            .file_type()
            .is_file()
        {
            return Err(failed("Numba object is not a regular file"));
        }
        let defined = run_tool(
            Command::new("nm").arg("--defined-only").arg(&path),
            work,
            &format!("symbols-{}", object.callback),
            Duration::from_secs(30),
            stop,
        )?;
        let defined: BTreeSet<_> = defined
            .lines()
            .filter_map(|line| line.split_whitespace().last())
            .collect();
        if !defined.contains(object.implementation.as_str()) {
            return Err(failed("Numba implementation is missing from object"));
        }
        let mut command = Command::new("objcopy");
        command.arg(format!(
            "--redefine-sym={}=titan_numba_impl_{}",
            object.implementation, object.callback
        ));
        for (index, name) in defined
            .iter()
            .filter(|name| name.starts_with("_ZN08NumbaEnv"))
            .enumerate()
        {
            command.arg(format!(
                "--redefine-sym={name}=titan_numba_env_{}_{index}",
                object.callback
            ));
        }
        run_tool(
            command.arg(&path),
            work,
            &format!("normalize-{}", object.callback),
            Duration::from_secs(30),
            stop,
        )?;
    }
    Ok(())
}

fn inspect_symbols(
    work: &Path,
    library: &Path,
    metadata: &NumbaMetadataV13,
    stop: &AtomicBool,
) -> Result<(), CliError> {
    let undefined = run_tool(
        Command::new("nm")
            .args(["-D", "--undefined-only"])
            .arg(library),
        work,
        "undefined",
        Duration::from_secs(30),
        stop,
    )?;
    if undefined
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .any(forbidden_symbol)
    {
        return Err(failed(
            "native library depends on Python/Numba runtime symbols",
        ));
    }
    let defined = run_tool(
        Command::new("nm")
            .args(["-D", "--defined-only"])
            .arg(library),
        work,
        "defined",
        Duration::from_secs(30),
        stop,
    )?;
    let actual: BTreeSet<_> = defined
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .filter(|name| name.starts_with("titan_strategy_"))
        .map(str::to_owned)
        .collect();
    let mut expected: BTreeSet<_> = metadata
        .callbacks
        .iter()
        .map(|name| format!("titan_strategy_{name}"))
        .collect();
    expected.extend([
        "titan_strategy_abi_version".to_owned(),
        "titan_strategy_descriptor".to_owned(),
    ]);
    if actual != expected {
        return Err(failed(
            "native callback exports do not match bridge metadata",
        ));
    }
    Ok(())
}

pub(super) fn compile(request: &CompileV13Request<'_>) -> Result<serde_json::Value, CliError> {
    let output = request
        .output
        .ok_or_else(|| failed("--output is required"))?;
    let target = request.target.unwrap_or(compiler_host_target_v13());
    validate_compile_target_v13(target, request.cpu_baseline).map_err(failed)?;
    if !matches!(request.artifact_format, "pair" | "bundle") {
        return Err(failed("invalid artifact format"));
    }
    if request.signing_key.is_some() != request.key_id.is_some()
        || request.key_id.is_some_and(|id| id.trim().is_empty())
    {
        return Err(failed(
            "--signing-key and a nonempty --key-id are required together",
        ));
    }
    let stop = Arc::new(AtomicBool::new(false));
    for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        signal_hook::flag::register(signal, stop.clone()).map_err(failed)?;
    }
    let mut seed = request.signing_key.map(read_seed).transpose()?;
    let result = compile_inner(request, output, target, seed.as_ref(), &stop);
    if let Some(seed) = &mut seed {
        // Keep key bytes out of the Python worker, its request, logs and artifact metadata.
        for byte in seed {
            unsafe {
                std::ptr::write_volatile(byte, 0);
            }
        }
    }
    result
}

fn compile_inner(
    request: &CompileV13Request<'_>,
    output: &Path,
    target: &str,
    seed: Option<&[u8; 32]>,
    stop: &AtomicBool,
) -> Result<serde_json::Value, CliError> {
    let source = fs::read(request.strategy).map_err(failed)?;
    let parameters: serde_json::Value =
        serde_json::from_slice(&fs::read(request.parameters).map_err(failed)?).map_err(failed)?;
    if !parameters.is_object() {
        return Err(failed("parameters JSON must be an object"));
    }
    let parent = output
        .parent()
        .filter(|v| !v.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(failed)?;
    let directory = tempfile::Builder::new()
        .prefix(".titan-compile-")
        .tempdir_in(parent)
        .map_err(failed)?;
    let work = fs::canonicalize(directory.path()).map_err(failed)?;
    // Both phases consume the same frozen source and parameters, including their digests.
    let snapshot = work.join("strategy.py");
    fs::write(&snapshot, &source).map_err(failed)?;
    let sdk = std::env::var_os("TITAN_STRATEGY_SDK")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("python/titan-strategy-sdk"));
    let sdk = fs::canonicalize(sdk).map_err(failed)?;
    let python = std::env::var_os("TITAN_STRATEGY_PYTHON").unwrap_or_else(|| "python3".into());
    let input = serde_json::json!({"protocol_version":1,"source":snapshot,"parameters":parameters,
        "cpu_baseline":request.cpu_baseline,"runtime_abi":{"abi_version":13,"fingerprint":hex_digest(&V13_ABI_FINGERPRINT)}});
    let input_path = work.join("request.json");
    fs::write(&input_path, compiler_json_v13(&input)).map_err(failed)?;
    let mut first = None;
    for (action, seconds) in [("describe", 60), ("emit-objects", 300)] {
        let mut command = Command::new(&python);
        command.env_clear();
        for name in [
            "PATH",
            "PYTHONHOME",
            "LANG",
            "LC_ALL",
            "TMPDIR",
            "LD_LIBRARY_PATH",
            "SYSTEMROOT",
        ] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command
            .env("PYTHONPATH", &sdk)
            .env("PYTHONHASHSEED", "0")
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("OPENBLAS_NUM_THREADS", "1")
            .env("NUMBA_NUM_THREADS", "1")
            .args(["-m", "titan_strategy.cli", action, "--request"])
            .arg(&input_path)
            .arg("--work-dir")
            .arg(&work);
        run_tool(
            &mut command,
            &work,
            action,
            Duration::from_secs(seconds),
            stop,
        )?;
        let metadata = read_metadata(&work.join(format!("{action}.json")))?;
        if let Some(expected) = &first {
            if expected != &metadata {
                return Err(failed(
                    "strategy metadata changed between describe and object emission",
                ));
            }
        } else {
            first = Some(metadata);
        }
    }
    let metadata = first.unwrap();
    normalize_objects(&work, &metadata, stop)?;
    let descriptor = work.join("descriptor.c");
    fs::write(&descriptor, metadata.descriptor_source().map_err(failed)?).map_err(failed)?;
    let descriptor_object = work.join("descriptor.o");
    run_tool(
        Command::new("cc")
            .args(["-std=c11", "-fPIC", "-O2", "-c"])
            .arg(&descriptor)
            .arg("-o")
            .arg(&descriptor_object),
        &work,
        "descriptor",
        Duration::from_secs(30),
        stop,
    )?;
    let mut exports = metadata
        .callbacks
        .iter()
        .map(|name| format!("    titan_strategy_{name};"))
        .collect::<Vec<_>>();
    exports.extend([
        "    titan_strategy_abi_version;".into(),
        "    titan_strategy_descriptor;".into(),
    ]);
    let version_script = work.join("native.exports");
    fs::write(
        &version_script,
        format!("{{ global:\n{}\n  local: *;\n}};\n", exports.join("\n")),
    )
    .map_err(failed)?;
    let library = work.join("native.so");
    let mut link = Command::new("cc");
    link.args(["-shared", "-Wl,--build-id=none"])
        .arg(format!("-Wl,--version-script={}", version_script.display()))
        .arg("-o")
        .arg(&library);
    for callback in &metadata.callbacks {
        let object = work.join(format!("{callback}.o"));
        link.arg(object);
    }
    // Each artifact must resolve its own native dependencies. A Python or large Rust host may
    // already have libm loaded and otherwise hide missing DT_NEEDED entries until deployment.
    link.arg(descriptor_object).args(["-Wl,-z,defs", "-lm"]);
    run_tool(&mut link, &work, "link", Duration::from_secs(30), stop)?;
    inspect_symbols(&work, &library, &metadata, stop)?;
    let library = fs::read(&library).map_err(failed)?;
    if stop.load(Ordering::Acquire) {
        return Err(failed("compilation interrupted"));
    }
    let artifact = write_artifact_v13(ArtifactBuildV13 {
        metadata: &metadata,
        library: &library,
        source_digest: Sha256::digest(&source).into(),
        parameters_digest: Sha256::digest(compiler_json_v13(&parameters)).into(),
        target_triple: target,
        cpu_baseline: request.cpu_baseline,
        bundle: request.artifact_format == "bundle",
        output,
        signing: request.key_id.zip(seed),
    })
    .map_err(failed)?;
    Ok(
        serde_json::json!({"strategy_id":artifact.manifest.strategy_id,"strategy_version":artifact.manifest.strategy_version,
        "artifact_digest":hex_digest(&artifact.manifest.artifact_digest),"native_digest":hex_digest(&artifact.manifest.native_digest),
        "paths":artifact.paths}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binary_signing_seeds_preserve_whitespace_and_hex_is_strict() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("key");
        let mut seed = [7; 32];
        seed[0] = b' ';
        seed[31] = b'\n';
        fs::write(&path, seed).unwrap();
        assert_eq!(read_seed(&path).unwrap(), seed);
        fs::write(&path, format!("{}\n", hex_digest(&seed))).unwrap();
        assert_eq!(read_seed(&path).unwrap(), seed);
        fs::write(&path, "é".repeat(32)).unwrap();
        assert!(read_seed(&path).is_err());
    }
    #[test]
    fn timeout_kills_worker_group_and_reaps_it() {
        let temp = tempfile::tempdir().unwrap();
        let mut command = Command::new("sh");
        let marker = temp.path().join("orphan-marker");
        command
            .args(["-c", "(sleep 0.3; touch \"$1\") & wait", "sh"])
            .arg(&marker);
        let start = Instant::now();
        assert!(
            run_tool(
                &mut command,
                temp.path(),
                "timeout",
                Duration::from_millis(40),
                &AtomicBool::new(false)
            )
            .unwrap_err()
            .to_string()
            .contains("limit")
        );
        assert!(start.elapsed() < Duration::from_secs(2));
        thread::sleep(Duration::from_millis(400));
        assert!(!marker.exists(), "timed-out worker left a live descendant");
    }
}
