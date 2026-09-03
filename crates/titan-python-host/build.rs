fn main() {
    println!("cargo:rerun-if-env-changed=PYO3_PYTHON");
    pyo3_build_config::add_python_framework_link_args();
    let config = pyo3_build_config::get();
    let Some(python) = config.executable.as_deref() else {
        return;
    };
    let Ok(output) = std::process::Command::new(python)
        .args([
            "-c",
            "import sysconfig; print(sysconfig.get_paths()['purelib'])",
        ])
        .output()
    else {
        return;
    };
    if output.status.success() {
        let site_packages = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !site_packages.is_empty() {
            println!("cargo:rustc-env=TITAN_PYTHON_SITE_PACKAGES={site_packages}");
        }
    }
    #[cfg(target_os = "macos")]
    stage_macos_python_dylib(config, python);
}

#[cfg(target_os = "macos")]
fn stage_macos_python_dylib(config: &pyo3_build_config::InterpreterConfig, python: &str) {
    use std::path::{Path, PathBuf};

    let Some(lib_name) = config.lib_name.as_deref() else {
        return;
    };
    let file_name = format!("lib{lib_name}.dylib");
    let mut candidates = Vec::new();
    if let Some(directory) = config.lib_dir.as_deref() {
        candidates.push(PathBuf::from(directory).join(&file_name));
    }
    if let Ok(output) = std::process::Command::new(python)
        .args([
            "-c",
            "import sysconfig; print(sysconfig.get_config_var('LIBDIR') or '')",
        ])
        .output()
        && output.status.success()
    {
        let directory = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !directory.is_empty() {
            candidates.push(PathBuf::from(directory).join(&file_name));
        }
    }
    let version = lib_name.trim_start_matches("python");
    for prefix in ["/opt/homebrew", "/usr/local"] {
        candidates.push(
            Path::new(prefix)
                .join(format!(
                    "opt/python@{version}/Frameworks/Python.framework/Versions/{version}/lib"
                ))
                .join(&file_name),
        );
    }
    let Some(source) = candidates.into_iter().find(|candidate| candidate.is_file()) else {
        println!(
            "cargo:warning=Python shared library {file_name} was not found; binaries may require an explicit runtime library path"
        );
        return;
    };
    let Some(profile_directory) = std::env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .and_then(|path| path.ancestors().nth(3).map(Path::to_path_buf))
    else {
        return;
    };
    let mut libraries = vec![source.clone()];
    if let Some(source_directory) = source.parent() {
        for dependency in ["libc++.1.dylib", "libiconv.2.dylib"] {
            let candidate = source_directory.join(dependency);
            if candidate.is_file() {
                libraries.push(candidate);
            }
        }
    }
    for directory in [profile_directory.clone(), profile_directory.join("deps")] {
        if std::fs::create_dir_all(&directory).is_err() {
            continue;
        }
        for library in &libraries {
            let Some(name) = library.file_name() else {
                continue;
            };
            let destination = directory.join(name);
            let matches_source = std::fs::canonicalize(&destination)
                .ok()
                .zip(std::fs::canonicalize(library).ok())
                .is_some_and(|(current, expected)| current == expected);
            if matches_source {
                continue;
            }
            if std::fs::symlink_metadata(&destination).is_ok() {
                let _ = std::fs::remove_file(&destination);
            }
            if std::os::unix::fs::symlink(library, &destination).is_err() {
                let _ = std::fs::copy(library, &destination);
            }
        }
    }
}
