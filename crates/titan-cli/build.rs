fn main() {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = std::process::Command::new(rustc)
        .arg("--version")
        .output()
        .expect("read compiler identity");
    assert!(output.status.success(), "read compiler identity");
    println!(
        "cargo:rustc-env=TITAN_RUSTC_VERSION={}",
        String::from_utf8_lossy(&output.stdout).trim()
    );
    println!("cargo:rerun-if-changed=build.rs");
}
