//! Short-lived management client; the Titan service owns running strategies.
fn main() -> std::process::ExitCode {
    std::process::ExitCode::from(titan_cli::management::run_frontend().clamp(0, 255) as u8)
}
