use std::{env, path::PathBuf, process::ExitCode};

fn main() -> ExitCode {
    let arguments = env::args_os().skip(1).map(PathBuf::from).collect::<Vec<_>>();
    if arguments.len() != 2 {
        eprintln!("usage: package_connector <library> <destination>");
        return ExitCode::from(2);
    }
    match titan_connector_loader::package_plugin_library(&arguments[0], &arguments[1]) {
        Ok(manifest) => {
            println!("{}", manifest.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("connector packaging failed: {error}");
            ExitCode::FAILURE
        }
    }
}
