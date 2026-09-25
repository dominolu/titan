"""Command line frontend for the offline Strategy ABI V13 compiler."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import sys

from . import abi_v13
from .static_compiler import CompileRequest, compile_package


def _host_target() -> str:
    machine = platform.machine().lower()
    system = platform.system().lower()
    if machine in ("x86_64", "amd64") and system == "linux":
        return "x86_64-unknown-linux-gnu"
    if machine in ("arm64", "aarch64") and system == "darwin":
        return "aarch64-apple-darwin"
    if machine in ("arm64", "aarch64") and system == "linux":
        return "aarch64-unknown-linux-gnu"
    return f"{machine}-unknown-{system}"


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="titan-strategy")
    commands = result.add_subparsers(dest="command", required=True)
    compile_parser = commands.add_parser("compile")
    compile_parser.add_argument("--strategy", required=True, type=Path)
    compile_parser.add_argument("--parameters", required=True, type=Path)
    compile_parser.add_argument("--target", default=_host_target())
    compile_parser.add_argument("--cpu-baseline", default="x86-64")
    compile_parser.add_argument("--artifact-format", choices=("pair", "bundle"), default="pair")
    compile_parser.add_argument("--output", required=True, type=Path)
    compile_parser.add_argument("--signing-key", type=Path,
                                help="file containing a 32-byte raw or 64-character hex Ed25519 private key")
    compile_parser.add_argument("--key-id")
    return result


def main() -> None:
    # Python chooses its hash secret before importing this module. Re-exec once with a fixed seed
    # so Numba/LLVM symbol discovery and manifest ordering remain reproducible across invocations.
    if os.environ.get("PYTHONHASHSEED") != "0":
        environment = dict(os.environ)
        environment["PYTHONHASHSEED"] = "0"
        os.execve(
            sys.executable,
            [sys.executable, "-m", "titan_strategy.cli", *sys.argv[1:]],
            environment,
        )
    arguments = parser().parse_args()
    parameters = json.loads(arguments.parameters.read_text(encoding="utf-8"))
    if not isinstance(parameters, dict):
        raise SystemExit("parameters JSON must be an object")
    signing_key = None
    if arguments.signing_key is not None:
        raw = arguments.signing_key.read_bytes().strip()
        if len(raw) == 64:
            try:
                raw = bytes.fromhex(raw.decode("ascii"))
            except (UnicodeDecodeError, ValueError) as error:
                raise SystemExit(f"invalid Ed25519 signing key: {error}") from error
        signing_key = raw
    result = compile_package(CompileRequest(
        source_file=arguments.strategy,
        parameters=parameters,
        target_triple=arguments.target,
        cpu_baseline=arguments.cpu_baseline,
        artifact_format=arguments.artifact_format,
        output_path=arguments.output,
        runtime_abi={"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()},
        signing_key=signing_key, signing_key_id=arguments.key_id,
    ))
    print(json.dumps({
        "strategy_id": result.strategy.definition.spec.strategy_id,
        "strategy_version": result.strategy.definition.spec.strategy_version,
        "artifact_digest": result.artifact.artifact_digest.hex(),
        "paths": [str(path) for path in result.artifact.paths],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
