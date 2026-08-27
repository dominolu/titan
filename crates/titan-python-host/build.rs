fn main() {
    println!("cargo:rerun-if-env-changed=PYO3_PYTHON");
    pyo3_build_config::add_python_framework_link_args();
    let Some(python) = pyo3_build_config::get().executable.as_deref() else {
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
}
