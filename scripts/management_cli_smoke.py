#!/usr/bin/env python3
"""Real managed CLI matrix. Execute only on configured titan-build.

python3 scripts/management_cli_smoke.py --repo /workspace/titan \
    --binary /workspace/titan/target/debug/titan --output /private/cli-smoke
Add --build for SDK Describe/freeze/native compilation. Add --lifecycle for the
existing signed native grid/control fixture (also needs openssl). Add --offline
--history-binary /workspace/titan/target/debug/titan-history --data /private/data
--parameters /private/parameters for the existing dataset/backtest/report fixture
(also needs the reporting Python environment). Python 3.11+, Linux /proc and the
server-built Titan binary are required. The output must be a new
private directory (outside the repository, or in ignored private storage).
Only allowlisted case names/status/hashes reach stdout. Raw CLI output, commands,
configuration and diagnostics remain private below --output. Optional lifecycle
uses only the existing loopback simulated venue, never a real venue or account.
"""
from __future__ import annotations

import argparse
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import signal
import sqlite3
import subprocess
import sys
import time
from urllib import error, request

# Reuse the management control smoke's fixture serialization and loopback ports.
from management_control_smoke import port, toml


class CheckFailure(Exception):
    """Only fixed, non-sensitive case identifiers may reach the public report."""


def require(condition, case):
    if not condition:
        raise CheckFailure(case)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def private_write(path, value):
    path.write_text(value if isinstance(value, str) else json.dumps(value, indent=2))
    path.chmod(0o600)


def wait_for(callback, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = callback()
            if value:
                return value
        except (error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(0.2)
    raise CheckFailure("fixture-condition-timeout")


class Matrix:
    def __init__(self, args):
        self.args, self.root = args, args.output
        self.processes, self.logs, self.checks = [], [], []
        self.sequence, self.paused = 0, False
        self.origin, self.node_port = f"http://127.0.0.1:{port()}", port()
        self.password = secrets.token_hex(24)
        # Never inherit an actual management connection from the operator's shell.
        excluded = {"TITAN_URL", "TITAN_USERNAME", "TITAN_PASSWORD_ENV", "TITAN_NODE",
                    "TITAN_ENVIRONMENT", "TITAN_PROFILE", "TITAN_CONFIG", "TITAN_CLIENT_CONFIG",
                    "TITAN_PERSISTENCE_STAGE", "TITAN_M3_PROFILE", "TITAN_M3_SERVICES"}
        self.env = {k: v for k, v in os.environ.items() if k not in excluded}
        self.env.update(TITAN_CLI_SMOKE_PASSWORD=self.password,
            TITAN_CONSOLE_NODE_TOKEN=secrets.token_hex(32),
            TITAN_STRATEGY_SDK=str(args.repo / "python/titan-strategy-sdk"),
            TITAN_STRATEGY_PYTHON=os.environ.get("TITAN_STRATEGY_PYTHON", "python3"),
            PYTHONDONTWRITEBYTECODE="1")
        self.http = request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.csrf = None

    def record(self, name):
        self.checks.append({"name": name, "status": "passed"})
        private_write(self.root / "checks.json", self.checks)
        print(json.dumps({"case": name, "status": "passed", "scope": "synthetic"}), flush=True)

    def raw(self, name, argv, *, input=None, timeout=90, interrupt=False):
        self.sequence += 1
        stem = self.root / f"{self.sequence:03d}-{name}"
        private_write(stem.with_suffix(".command.json"), {"argv": list(map(str, argv))})
        started = time.monotonic()
        with stem.with_suffix(".stdout").open("wb") as out, stem.with_suffix(".stderr").open("wb") as err:
            if interrupt:
                require(input is None, "interrupt-stdin-unused")
                child = subprocess.Popen(list(map(str, argv)), stdout=out, stderr=err,
                    stdin=subprocess.DEVNULL, env=self.env, cwd=self.args.repo)
                try:
                    def signal_ready():
                        require(child.poll() is None, "interrupt-child-exited-before-observation")
                        lines = Path(f"/proc/{child.pid}/status").read_text().splitlines()
                        caught = next(line.split()[1] for line in lines if line.startswith("SigCgt:"))
                        return bool(int(caught, 16) & (1 << (signal.SIGINT - 1))) and (
                            interrupt != "after-output" or stem.with_suffix(".stdout").stat().st_size > 0)
                    wait_for(signal_ready, timeout=15)
                    child.send_signal(signal.SIGINT)
                    result_code = child.wait(timeout=timeout)
                finally:
                    if child.poll() is None:
                        child.kill()
                        child.wait(timeout=5)
            else:
                result = subprocess.run(list(map(str, argv)), input=input, stdout=out, stderr=err,
                                        timeout=timeout, env=self.env, cwd=self.args.repo)
                result_code = result.returncode
        private_write(stem.with_suffix(".result.json"), {
            "exit_code": result_code, "elapsed_seconds": round(time.monotonic() - started, 3)})
        return result_code, stem.with_suffix(".stdout").read_text(), stem.with_suffix(".stderr").read_text()

    def cli(self, name, *words, profile="operator", code=0, error_code=None, timeout=90, overrides=(), interrupt=False, machine="json", expected_kind=None):
        argv = [self.args.binary, "console", "--client-config", self.root / "client.toml",
                "--profile", profile, "--" + machine, *overrides, *words]
        actual, stdout, stderr = self.raw(name, argv, timeout=timeout, interrupt=interrupt)
        require(actual in code if isinstance(code, tuple) else actual == code, name + "-exit")
        output = None
        if stdout.strip():
            try:
                envelopes = [json.loads(line) for line in stdout.splitlines()] if machine == "ndjson" else [json.loads(stdout)]
            except ValueError:
                raise CheckFailure(name + "-stdout-json") from None
            require(all(isinstance(item, dict) and item.get("schema_version") == 1 and item.get("api_version") == 2
                        for item in envelopes), name + "-envelope")
            if expected_kind:
                require(all(item.get("kind") == expected_kind for item in envelopes), name + "-kind")
            output = [item["data"] for item in envelopes] if machine == "ndjson" else envelopes[0]["data"]
        if error_code and actual != 0:
            envelopes = []
            for line in stderr.splitlines():
                try:
                    envelopes.append(json.loads(line))
                except ValueError:
                    continue
            require(any(item.get("kind") == "error" and item.get("data", {}).get("code") == error_code
                        for item in envelopes), name + "-error-code")
        require(self.password not in stdout + stderr, name + "-password-disclosure")
        require(self.env["TITAN_CONSOLE_NODE_TOKEN"] not in stdout + stderr, name + "-token-disclosure")
        return output

    def call(self, path, data=None):
        """HTTP is fixture setup and independent fact comparison only."""
        headers = {"Origin": self.origin, "Content-Type": "application/json"}
        if self.csrf:
            headers["X-CSRF-Token"] = self.csrf
        with self.http.open(request.Request(self.origin + path,
                data=None if data is None else json.dumps(data).encode(), headers=headers), timeout=15) as response:
            return json.load(response)

    def start(self, kind):
        log = (self.root / (kind + ".log")).open("ab")
        self.logs.append(log)
        process = subprocess.Popen([str(self.args.binary), "console", kind, "--config", str(self.root / "console.toml")],
            env=self.env, cwd=self.args.repo, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        self.processes.append(process)
        return process

    def setup(self):
        code, hashed, _ = self.raw("password-hash", [self.args.binary, "console", "password-hash"],
                                   input=(self.password + "\n").encode())
        require(code == 0 and hashed.strip().startswith("$argon2"), "password-hash")
        private_write(self.root / "parameters.json", {})
        private_write(self.root / "invalid-parameters.json", {"unexpected": 1})
        signing = self.root / "signing.key"
        signing.write_bytes(secrets.token_bytes(32))
        signing.chmod(0o600)
        node = {"id": "build", "listen": f"127.0.0.1:{self.node_port}",
            "token_env": "TITAN_CONSOLE_NODE_TOKEN", "work_root": str(self.root / "node"),
            "titan_program": str(self.args.binary), "environments": ["backtest"], "task_seconds": 900,
            "signing_key": str(signing), "signing_key_id": "synthetic",
            "parameter_presets": {name: {"strategy_id": strategy, "strategy_name": strategy,
                "source": str(self.args.repo / "strategies/grid.py"), "parameters": str(self.root / "parameters.json")}
                for name, strategy in (("grid", "grid"), ("foreign", "foreign"))}}
        users = [{"username": name, "password_hash": hashed.strip(), "role": role,
            "nodes": nodes, "accounts": [], "environments": ["backtest"]}
            for name, role, nodes in (("operator", "operator", ["build"]),
                                     ("reader", "reader", ["build"]), ("hidden", "reader", []))]
        manager = {"listen": self.origin.removeprefix("http://"), "public_origin": self.origin,
            "database": str(self.root / "manager.db"), "web_root": str(self.args.repo / "web/console/dist"),
            "secure_cookie": False, "nodes": {"build": {"endpoint": f"http://127.0.0.1:{self.node_port}",
            "token_env": "TITAN_CONSOLE_NODE_TOKEN", "environments": ["backtest"]}}, "users": users}
        private_write(self.root / "console.toml", toml({"schema_version": 1, "manager": manager, "node": node}))
        profiles = {name: {"url": self.origin, "username": name, "password_env": "TITAN_CLI_SMOKE_PASSWORD",
            "node": "build", "environment": "backtest"} for name in ("operator", "reader", "hidden")}
        private_write(self.root / "client.toml", toml({"schema_version": 1, "default_profile": "operator", "profiles": profiles}))
        self.node, self.manager = self.start("node"), self.start("manager")
        session = wait_for(lambda: self.call("/api/v2/session", {"username": "operator", "password": self.password}))
        self.csrf = session["csrf_token"]
        wait_for(lambda: len([r for r in self.call("/api/v2/snapshot")["resources"] if r["kind"] == "parameter_preset"]) == 2)
        # Inert profile: no credentials, runtime or connector is configured.
        self.account = self.call("/api/v2/accounts", {"node_id": "build", "name": "synthetic-account",
            "exchange": "gate", "environment": "backtest", "credential_ref": "", "notes": "synthetic fixture",
            "enabled": True, "expected_revision": None})
        private_write(self.root / "fixture-account.json", self.account)

    def pause(self):
        self.node.send_signal(signal.SIGSTOP)
        self.paused = True

    def resume(self):
        if self.paused:
            self.node.send_signal(signal.SIGCONT)
            self.paused = False

    def operation(self, ident):
        return self.call("/api/v2/operations/" + ident)

    def describe(self, name, key, **kwargs):
        return self.cli(name, "strategy", "describe", "--parameter-preset-id", "grid", "--parameters",
                        self.root / "parameters.json", "--idempotency-key", key, **kwargs)

    def base(self):
        for action in ("login", "check", "logout"):
            self.cli("profile-" + action, action)
        self.cli("reader-login", "login", profile="reader")
        self.cli("profile-explicit-override", "login", profile="hidden", overrides=("--username", "reader"))
        self.cli("missing-profile", "login", profile="missing", code=2, error_code="INVALID_CONFIG")
        self.record("profile-login-check-logout-and-explicit-precedence")
        self.cli("node-list", "node", "ls")
        self.cli("status", "status")
        self.observer_checks()
        strategies = self.cli("strategy-list", "strategy", "ls", "--limit", "1")
        other = self.cli("strategy-page-two", "strategy", "ls", "--limit", "1", "--offset", "1")
        require(isinstance(strategies, list) and len(strategies) == len(other) == 1, "strategy-server-page")
        require(strategies[0]["id"] != other[0]["id"], "strategy-distinct-pages")
        self.cli("strategy-show", "strategy", "show", "grid")
        presets = self.cli("strategy-presets", "strategy", "presets", "--strategy-id", "grid")
        require(len(presets) == 1 and presets[0]["body"]["parameter_preset_id"] == "grid", "strategy-preset-isolation")
        require(self.cli("hidden-strategies", "strategy", "ls", profile="hidden") == [], "hidden-strategies")
        self.record("strategy-directory-pagination-and-authorization")
        self.account_checks()
        draft = self.cli("parameter-create", "parameter", "create", "--parameter-preset-id", "grid",
                         "--parameters", self.root / "parameters.json")
        updated = self.cli("parameter-update", "parameter", "update", draft["id"], "--parameters",
                           self.root / "invalid-parameters.json", "--expected-revision", draft["revision"])
        self.cli("parameter-stale-revision", "parameter", "update", draft["id"], "--parameters",
                 self.root / "parameters.json", "--expected-revision", draft["revision"], code=5)
        require(updated["revision"] != draft["revision"], "parameter-revision-advanced")
        self.scope_checks(updated)
        self.cli("reader-parameter-denied", "parameter", "create", "--parameter-preset-id", "grid",
                 "--parameters", self.root / "parameters.json", profile="reader", code=3)
        self.record("parameter-draft-revision-conflict-and-reader-denial")
        self.pause()
        try:
            pending = self.describe("operation-submit", "synthetic-once")
            require(pending["state"] == "pending", "accepted-is-pending")
            require(self.describe("operation-repeat", "synthetic-once")["id"] == pending["id"], "same-key-same-operation")
            self.cli("operation-conflict", "strategy", "describe", "--parameter-preset-id", "grid",
                "--parameters", self.root / "invalid-parameters.json", "--idempotency-key", "synthetic-once", code=5)
            found = self.cli("operation-find-key", "operation", "by-key", "synthetic-once")
            require(isinstance(found, dict) and found["id"] == pending["id"], "key-find")
            require(self.cli("operation-key-missing", "operation", "by-key", "synthetic-missing") is None,
                    "key-missing-is-null")
            legacy_path = self.root / "legacy-request.json"
            private_write(legacy_path, {"node_id": "build", "idempotency_key": "synthetic-once", "action": {
                "kind": "describe", "parameter_preset_id": "grid", "parameters": {}}})
            legacy_code, legacy_out, legacy_err = self.raw("legacy-request-raw", [self.args.binary,
                "console", "request", "--url", self.origin, "--username", "operator", "--password-env",
                "TITAN_CLI_SMOKE_PASSWORD", "--request", legacy_path])
            require(legacy_code == 0 and json.loads(legacy_out)["id"] == pending["id"], "legacy-original-key-identity")
            require(self.password not in legacy_out + legacy_err, "legacy-password-disclosure")
            require(self.cli("legacy-request-json", "request", "--request", legacy_path)["id"] == pending["id"],
                    "legacy-json-envelope")
            self.cli("operation-show", "operation", "show", pending["id"])
            self.cli("operation-list-page", "operation", "ls", "--limit", "1", "--offset", "0")
            self.cli("operation-wait-timeout", "operation", "wait", pending["id"], "--timeout-seconds", "1",
                     code=8, error_code="WAIT_TIMEOUT")
            self.cli("operation-wait-interrupt", "operation", "wait", pending["id"], "--timeout-seconds", "30",
                     code=130, error_code="INTERRUPTED", interrupt=True)
            after = self.operation(pending["id"])
            require(after["state"] == "pending" and not after["cancel_requested"], "timeout-and-interrupt-preserve-operation")
            self.cli("reader-cancel-denied", "operation", "cancel", pending["id"], profile="reader", code=3)
            cancelled = self.cli("operation-request-cancel", "operation", "cancel", pending["id"])
            require(cancelled["cancel_requested"] and cancelled["state"] == "pending", "cancel-is-request-only")
        finally:
            self.resume()
        wait_for(lambda: self.operation(pending["id"])["state"] == "cancelled")
        self.cli("operation-wait-cancelled", "operation", "wait", pending["id"], "--timeout-seconds", "5",
                 code=7, error_code="OPERATION_CANCELLED")
        with sqlite3.connect(self.root / "manager.db") as db:
            require(db.execute("SELECT COUNT(*) FROM operations WHERE actor='operator' AND key='synthetic-once'").fetchone()[0] == 1,
                    "idempotency-unique-store")
        self.record("operation-original-key-conflict-timeout-and-predispatch-cancel")
        self.unknown()

    def scope_checks(self, draft):
        for option, value in (("--node", "other"), ("--environment", "live")):
            label = "node" if option == "--node" else "environment"
            require(self.cli("strategy-filter-" + label, "strategy", "ls", "--limit", "1",
                             overrides=(option, value)) == [], "strategy-empty-scope-" + label)
            self.cli("strategy-detail-filter-" + label, "strategy", "show", "grid", overrides=(option, value), code=4)
            require(self.cli("parameter-filter-" + label, "parameter", "ls", "--strategy-id", "grid",
                             overrides=(option, value)) == [], "parameter-empty-scope-" + label)
            self.cli("parameter-show-scope-" + label, "parameter", "show", draft["id"],
                     overrides=(option, value), code=2, error_code="INVALID_INPUT")
            self.cli("parameter-copy-scope-" + label, "parameter", "copy", draft["id"],
                     overrides=(option, value), code=2, error_code="INVALID_INPUT")
            self.cli("parameter-update-scope-" + label, "parameter", "update", draft["id"],
                     "--parameters", self.root / "parameters.json", "--expected-revision", draft["revision"],
                     overrides=(option, value), code=2, error_code="INVALID_INPUT")
            self.cli("parameter-freeze-scope-" + label, "parameter", "freeze", draft["id"],
                     "--expected-revision", draft["revision"], "--freeze-operation-id", "synthetic-freeze-evidence",
                     overrides=(option, value), code=2, error_code="INVALID_INPUT")
            self.cli("account-show-scope-" + label, "account", "show", self.account["id"],
                     overrides=(option, value), code=2, error_code="INVALID_INPUT")
            listed = self.cli("account-filter-" + label, "account", "ls", overrides=(option, value))
            require(not listed["profiles"]["items"], "account-empty-scope-" + label)
        self.cli("parameter-create-environment-mismatch", "parameter", "create", "--parameter-preset-id", "grid",
                 "--parameters", self.root / "parameters.json", overrides=("--environment", "live"),
                 code=2, error_code="INVALID_INPUT")
        unchanged = self.cli("parameter-scope-no-mutation", "parameter", "show", draft["id"])
        require(unchanged["revision"] == draft["revision"], "scope-rejection-no-parameter-mutation")
        self.record("explicit-node-environment-scope-and-write-rejection")

    def observer_checks(self):
        with sqlite3.connect(self.root / "manager.db") as db:
            before = db.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
        frames = self.cli("watch-ndjson", "watch", "--samples", "3", "--interval-ms", "250",
                          machine="ndjson", expected_kind="watch")
        require(len(frames) == 3 and all(frame["state"] == "snapshot" for frame in frames), "watch-three-frames")
        self.cli("watch-json-invalid-stream", "watch", "--samples", "2", code=2, error_code="INVALID_INPUT")
        self.cli("machine-output-conflict", "check", overrides=("--ndjson",), code=2)
        frames = self.cli("watch-interrupt", "watch", "--samples", "100", "--interval-ms", "250",
                          machine="ndjson", expected_kind="watch", interrupt="after-output")
        require(len(frames) >= 1, "watch-frame-before-interrupt")
        with sqlite3.connect(self.root / "manager.db") as db:
            require(db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == before,
                    "watch-interruption-no-remote-control")
        private_write(self.root / "invalid-request.json", {"node_id": "build", "action": {}})
        self.cli("operation-invalid-input", "operation", "submit", "--request", self.root / "invalid-request.json",
                 code=2, error_code="INVALID_INPUT")
        self.record("strict-ndjson-stream-invalid-mode-and-observer-interruption")

    def account_checks(self):
        listed = self.cli("account-list", "account", "ls")
        require(any(r["id"] == self.account["id"] for r in listed["profiles"]["items"]), "account-profile-listed")
        shown = self.cli("account-show", "account", "show", self.account["id"])
        require("credential_ref" not in json.dumps(shown) and "notes" not in json.dumps(shown), "account-public-projection")
        for kind in ("balances", "positions", "orders", "risk"):
            value = self.cli("account-" + kind, "account", kind, self.account["id"])
            require(value["available"] is False and value["data"] is None, "account-no-fabricated-" + kind)
        self.cli("reader-account-list", "account", "ls", profile="reader")
        hidden = self.cli("hidden-account-list", "account", "ls", profile="hidden")
        require(not hidden["profiles"]["items"], "hidden-account-profiles")
        self.record("synthetic-account-profile-has-no-live-financial-facts")

    def unknown(self):
        self.pause()
        try:
            op = self.describe("unknown-submit", "synthetic-unknown")
            require(op["state"] == "pending", "unknown-initial-pending")
            # Missing receipt with admission evidence is an explicit fault injection.
            # Actual node returns 503; actual manager classifies UNKNOWN. No stored
            # manager state or engine result is rewritten to manufacture a terminal.
            directory = self.root / "node/operations" / op["id"]
            directory.mkdir()
            private_write(self.root / "fault-injection.json", {"kind": "admission-evidence-without-receipt",
                          "operation_id": op["id"], "node_directory": str(directory)})
        finally:
            self.resume()
        wait_for(lambda: self.operation(op["id"])["state"] == "unknown")
        require(self.cli("unknown-show", "operation", "show", op["id"])["state"] == "unknown", "unknown-visible")
        self.cli("unknown-wait", "operation", "wait", op["id"], "--timeout-seconds", "5", code=7, error_code="OPERATION_UNKNOWN")
        require(self.describe("unknown-repeat", "synthetic-unknown")["id"] == op["id"], "unknown-preserves-operation")
        require(not (directory / "request.json").exists(), "unknown-no-redispatch")
        self.record("missing-node-receipt-is-unknown-without-redispatch")

    def reobserve(self, name, receipt, key, deadline):
        require(receipt["state"] in ("succeeded", "unknown"), name + "-unexpected-wait-result")
        if receipt["state"] == "succeeded":
            return receipt
        ident, identity = receipt["id"], receipt["input_digest"]
        trace = [{"state": "unknown", "source": "initial-cli-wait"}]
        while time.monotonic() < deadline:
            found = self.cli(name + "-original-key", "operation", "by-key", key)
            require(isinstance(found, dict) and found["id"] == ident and found["input_digest"] == identity,
                    name + "-original-key-identity")
            latest = self.cli(name + "-reobserve", "operation", "get", ident)
            require(latest["id"] == ident and latest["input_digest"] == identity, name + "-reobserve-identity")
            trace.append({"state": latest["state"], "revision": latest["revision"], "source": "cli-get"})
            private_write(self.root / (name + "-reconciliation.json"), {
                "operation_id": ident, "idempotency_key": key, "input_digest": identity, "observations": trace})
            if latest["state"] == "succeeded":
                final = self.cli(name + "-reconciled-wait", "operation", "wait", ident, "--timeout-seconds", "5")
                require(final["id"] == ident and final["state"] == "succeeded", name + "-reconciled-terminal")
                self.record(name + "-unknown-reconciled-by-original-key")
                return final
            require(latest["state"] in ("pending", "running", "unknown"), name + "-reconciled-business-failure")
            time.sleep(0.5)
        raise CheckFailure(name + "-reconciliation-deadline")

    def build(self):
        code, text, _ = self.raw("local-describe-defaults", [self.args.binary, "strategy", "describe",
            self.args.repo / "strategies/grid.py", "--json"], timeout=180)
        require(code == 0, "local-describe-defaults")
        private_write(self.root / "parameters.json", json.loads(text)["normalized_parameters"])
        describe_deadline = time.monotonic() + 180
        described = self.cli("managed-describe", "strategy", "describe", "--parameter-preset-id", "grid",
            "--parameters", self.root / "parameters.json", "--idempotency-key", "synthetic-describe",
            "--wait", "--timeout-seconds", "180", timeout=210, code=(0, 7), error_code="OPERATION_UNKNOWN")
        described = self.reobserve("managed-describe", described, "synthetic-describe", describe_deadline)
        require(described["state"] == "succeeded", "managed-describe-succeeded")
        require(described == self.operation(described["id"]), "describe-http-fact-comparison")
        draft = self.cli("build-parameter-create", "parameter", "create", "--parameter-preset-id", "grid",
                         "--parameters", self.root / "parameters.json")
        frozen = self.cli("parameter-freeze", "parameter", "freeze", draft["id"], "--expected-revision",
                          draft["revision"], "--freeze-operation-id", described["id"])
        require(frozen["frozen"] is True, "frozen-parameter")
        self.cli("frozen-update-denied", "parameter", "update", frozen["id"], "--expected-revision",
                 frozen["revision"], "--parameters", self.root / "parameters.json", code=5)
        compile_deadline = time.monotonic() + 900
        compiled = self.cli("managed-compile", "strategy", "compile", "--parameter-version-id", frozen["id"],
            "--idempotency-key", "synthetic-compile", "--wait", "--timeout-seconds", "900", timeout=940,
            code=(0, 7), error_code="OPERATION_UNKNOWN")
        compiled = self.reobserve("managed-compile", compiled, "synthetic-compile", compile_deadline)
        require(compiled["state"] == "succeeded", "managed-compile-succeeded")
        versions = [r for r in compiled["result"]["resources"] if r["kind"] == "strategy_version"]
        require(len(versions) == 1, "compiled-version-resource")
        self.cli("strategy-versions", "strategy", "versions", "--strategy-id", "grid")
        self.cli("strategy-version", "strategy", "version", "grid", versions[0]["id"])
        with sqlite3.connect(self.root / "node/runs.sqlite3") as db:
            require(db.execute("SELECT COUNT(*) FROM runs WHERE id=?", (compiled["id"],)).fetchone()[0] == 1, "compiled-one-worker")
        self.record("real-describe-freeze-signed-compile-and-immutable-parameters")

    def lifecycle(self):
        code, _, _ = self.raw("control-cli-driver", [sys.executable, Path(__file__).resolve(),
            "--repo", self.args.repo, "--binary", self.args.binary,
            "--output", self.root / "lifecycle", "--control-driver",
            *(["--client-binary", self.args.client_binary] if self.args.client_binary else [])], timeout=1200)
        require(code == 0, "control-cli-driver")
        result = json.loads((self.root / "lifecycle/results.json").read_text())
        require(result["passed"] and result["remaining_orders"] == 0, "control-cli-runtime-release")
        require(json.loads((self.root / "lifecycle/cli-observation-checks.json").read_text())["passed"],
                "control-cli-runtime-observation-queries")
        self.record("existing-control-fixture-through-real-cli-lifecycle-commands")
        self.record("real-runtime-cli-facts-generation-cursors-and-log-follow")
        if self.args.client_binary:
            routed = json.loads((self.root / "lifecycle/frontend-route-evidence.json").read_text())
            kinds = {item["action"]["kind"] for item in routed["mapped_original_fixture_keys"].values()}
            require({"start", "pause", "resume", "stop", "restore"}.issubset(kinds), "new-frontend-all-lifecycle-actions-routed")
            self.record("new-frontend-strategy-controls-with-compatible-explicit-negatives")

    def offline(self):
        code, _, _ = self.raw("offline-cli-driver", [sys.executable, Path(__file__).resolve(),
            "--repo", self.args.repo, "--binary", self.args.binary, "--output", self.root / "offline",
            "--history-binary", self.args.history_binary, "--data", self.args.data,
            "--parameters", self.args.parameters, "--offline-driver",
            *(["--client-binary", self.args.client_binary] if self.args.client_binary else [])], timeout=2400)
        require(code == 0, "offline-cli-driver")
        require(json.loads((self.root / "offline/results.json").read_text())["passed"], "offline-cli-history-result")
        self.record("existing-offline-fixture-through-real-cli-prepare-backtest-report")
        if self.args.client_binary:
            routed = json.loads((self.root / "offline/frontend-route-evidence.json").read_text())
            kinds = {item["action"]["kind"] for item in routed["mapped_original_fixture_keys"].values()}
            require({"backtest", "report"}.issubset(kinds), "new-frontend-offline-actions-routed")
            self.record("new-frontend-research-backtest-report-with-compatible-fixture-setup")

    def close(self):
        self.resume()
        for process in reversed(self.processes):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
        for log in self.logs:
            log.close()


def check_runtime_queries(args, session, run, opener):
    """Probe observation commands against the existing live synthetic worker."""
    run_id, generation = run["id"], run["body"]["run_generation"]
    counter = 0

    def cli(name, *words, code=0, error_code=None, ndjson=False):
        nonlocal counter
        counter += 1
        argv = [str(args.binary), "console", "--url", session["origin"], "--username", session["username"],
            "--password-env", "TITAN_CLI_DRIVER_PASSWORD", "--node", run["node_id"], "--environment", "live",
            "--ndjson" if ndjson else "--json", *map(str, words)]
        result = subprocess.run(argv, capture_output=True, timeout=45,
            env={**os.environ, "TITAN_CLI_DRIVER_PASSWORD": session["password"]})
        private_write(args.output / f"observation-{counter:03d}-{name}.json", {
            "argv": argv, "exit_code": result.returncode,
            "stdout": result.stdout.decode(), "stderr": result.stderr.decode()})
        require(result.returncode == code, "runtime-query-" + name + "-exit")
        if error_code:
            errors = []
            for line in result.stderr.splitlines():
                try:
                    errors.append(json.loads(line))
                except ValueError:
                    pass
            require(any(item.get("data", {}).get("code") == error_code for item in errors),
                    "runtime-query-" + name + "-error")
        if not result.stdout.strip():
            return None
        envelopes = [json.loads(line) for line in result.stdout.splitlines()] if ndjson else [json.loads(result.stdout)]
        require(all(item.get("schema_version") == 1 and item.get("api_version") == 2 for item in envelopes),
                "runtime-query-" + name + "-envelope")
        return [item["data"] for item in envelopes] if ndjson else envelopes[0]["data"]

    def account_source():
        with opener.open(session["origin"] + "/api/v2/resources/" + run_id, timeout=15) as response:
            value = json.load(response)
        require(value["id"] == run_id and value["body"]["run_generation"] == generation,
                "runtime-account-source-identity")
        return value

    for field in ("balances", "positions", "orders", "risk"):
        def matches_source():
            source_before = account_source()
            projected = cli("account-" + field, "account", field, run_id)
            source_after = account_source()
            before_snapshot = source_before["body"].get("observation", {}).get("account_snapshot", {})
            after_snapshot = source_after["body"].get("observation", {}).get("account_snapshot", {})
            before_image = before_snapshot.get("image", {})
            after_image = after_snapshot.get("image", {})
            private_write(args.output / ("account-" + field + "-comparison.json"), {
                "before": source_before, "cli": projected, "after": source_after})
            if not (before_snapshot.get("available") and after_snapshot.get("available")):
                return False
            if before_image.get(field) != after_image.get(field):
                return False
            # Direct equality retains original decimal strings, NET/LONG/SHORT
            # identities and nulls. No numeric conversions or client accounting.
            expected = before_image.get(field)
            return projected["data"] == expected and (expected is None or projected["available"] is True)
        wait_for(matches_source, timeout=15)

    with sqlite3.connect(args.output / "manager.db") as db:
        before = db.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
    for command in ("facts", "history", "logs"):
        cli(command + "-wrong-generation", command, run_id, "--generation", "synthetic-wrong",
            code=5, error_code="STALE_GENERATION")
    current = cli("facts-current", "facts", run_id, "--limit", "1")
    require(current["source_generation_guard"] == generation and current["fact_run_generation_filter"] == generation,
            "runtime-facts-current-generation-default")
    old = cli("facts-old-filter", "facts", run_id, "--run-generation", "synthetic-old", "--limit", "1")
    require(old["fact_run_generation_filter"] == "synthetic-old" and not old["page"]["items"], "runtime-facts-old-filter")
    empty = cli("facts-signed-time", "facts", run_id, "--from-ns", "-2", "--to-ns", "10", "--limit", "1")
    require(not empty["page"]["items"] and empty["time_basis"] == "received_at_ns inclusive", "runtime-facts-signed-time")
    bounds = ("--from-ns", "0", "--to-ns", "9223372036854775807", "--limit", "1")
    def export_window():
        first = cli("facts-first-page", "facts", run_id, *bounds)
        if not first["page"].get("next_cursor"):
            return None
        following = cli("facts-window-probe", "facts", run_id, *bounds,
                        "--cursor", json.dumps(first["page"]["next_cursor"]))
        return first if following["page"].get("next_cursor") else None
    page = wait_for(export_window, timeout=15)
    cursor = json.dumps(page["page"]["next_cursor"])
    continued = cli("facts-next-page", "facts", run_id, *bounds, "--cursor", cursor)
    require(continued["page"]["upper_row"] == page["page"]["upper_row"], "runtime-facts-fixed-upper")
    require(not {row["row_id"] for row in page["page"]["items"]}.intersection(
        row["row_id"] for row in continued["page"]["items"]), "runtime-facts-distinct-pages")
    exported = cli("facts-bounded-export", "facts", run_id, *bounds, "--cursor", cursor,
                   "--max-pages", "2", ndjson=True)
    require(len(exported) == 2, "runtime-facts-two-export-pages")
    require(all(frame["page"]["upper_row"] == page["page"]["upper_row"]
                and frame["fact_run_generation_filter"] == generation
                and frame["source_generation_guard"] == generation for frame in exported),
            "runtime-facts-export-fixed-upper-and-generation")
    row_ids = [row["row_id"] for frame in exported for row in frame["page"]["items"]]
    require(len(row_ids) == len(set(row_ids)), "runtime-facts-export-no-duplicates")
    require(exported[0]["page"]["items"] == continued["page"]["items"]
            and exported[0]["page"].get("next_cursor") == continued["page"].get("next_cursor"),
            "runtime-facts-export-original-cursor-and-time")
    require(all("export" in frame and frame["export"]["max_pages"] == 2 for frame in exported),
            "runtime-facts-export-bounds-metadata")
    cli("facts-json-invalid-export", "facts", run_id, "--max-pages", "2", code=2, error_code="INVALID_INPUT")
    cli("facts-cursor-time-conflict", "facts", run_id, "--from-ns", "1", "--to-ns", "9223372036854775807",
        "--limit", "1", "--cursor", cursor, code=5, error_code="STALE_HISTORY_CURSOR")
    cli("facts-cursor-generation-conflict", "facts", run_id, *bounds, "--run-generation", "synthetic-old",
        "--cursor", cursor, code=5, error_code="STALE_HISTORY_CURSOR")
    log = cli("logs-first-page", "logs", run_id, "--offset", "0")
    offset = int(log["page"]["next_offset"])
    require(0 <= offset <= 65536, "runtime-log-first-page-byte-bound")
    again = cli("logs-next-page", "logs", run_id, "--offset", str(offset))
    require(offset <= int(again["page"]["next_offset"]) <= offset + 65536, "runtime-log-next-page-byte-bound")
    frames = cli("logs-follow", "logs", run_id, "--follow", "--samples", "2", "--interval-ms", "250", ndjson=True)
    require(len(frames) == 2 and [frame["sample"] for frame in frames] == [0, 1], "runtime-log-follow-ndjson")
    cli("logs-json-invalid-stream", "logs", run_id, "--follow", "--samples", "2", code=2, error_code="INVALID_INPUT")
    with sqlite3.connect(args.output / "manager.db") as db:
        require(db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == before, "runtime-observers-no-control")
    private_write(args.output / "cli-observation-checks.json", {"passed": True, "cases": [
        "account-projections-equal-stable-runtime-facts", "generation-guard", "run-generation-filter",
        "signed-inclusive-time", "cursor-fixed-upper-and-conflicts", "bounded-ndjson-facts-export",
        "log-byte-pages-and-ndjson-follow", "no-remote-control"]})


def fixture_driver(args, offline=False):
    """Keep existing fixture/business assertions; route its operation writes to CLI.

    HTTP session setup, parameter freezing and reads remain the original fixture's
    setup/fact comparison. All Operation submission/cancellation uses a real Titan
    process and specialized commands. No engine implementation is duplicated.
    """
    import io
    from urllib.parse import urlparse
    import management_console_smoke
    import management_control_smoke

    original = request.OpenerDirector.open
    sessions = {}
    frontend_journals = {}
    counter = 0
    observed_runtime = False
    def open_via_cli(opener, resource, *positional, **keywords):
        nonlocal counter, observed_runtime
        if (not offline and isinstance(resource, request.Request) and resource.get_method() == "GET"
                and urlparse(resource.full_url).path.startswith("/api/v2/operations/")):
            with original(opener, resource, *positional, **keywords) as response:
                payload = response.read()
            value = json.loads(payload)
            if (not observed_runtime and value.get("state") == "succeeded"
                    and value.get("input", {}).get("kind") == "start"):
                runs = [item for item in value["result"]["resources"] if item["kind"] == "run"]
                require(len(runs) == 1, "runtime-observation-start-resource")
                observed_runtime = True
                check_runtime_queries(args, sessions[opener], runs[0], opener)
            return io.BytesIO(payload)
        if not isinstance(resource, request.Request) or resource.get_method() != "POST":
            return original(opener, resource, *positional, **keywords)
        url = urlparse(resource.full_url)
        if url.path == "/api/v2/session":
            credentials = json.loads(resource.data)
            sessions[opener] = {"origin": f"{url.scheme}://{url.netloc}", **credentials}
            return original(opener, resource, *positional, **keywords)
        session = sessions.get(opener, {})
        cancel = url.path.startswith("/api/v2/operations/") and url.path.endswith("/cancel")
        if url.path != "/api/v2/operations" and not cancel:
            return original(opener, resource, *positional, **keywords)
        require(url.hostname == "127.0.0.1" and session.get("origin") == f"{url.scheme}://{url.netloc}",
                "fixture-driver-loopback-origin")
        body = json.loads(resource.data or b"{}")
        if cancel:
            words = ["operation", "cancel", url.path.split("/")[-2]]
            node, environment = "build", "backtest"
        else:
            action, node = body["action"], body["node_id"]
            kind = action["kind"]
            environment = "backtest" if kind in ("describe", "compile", "prepare", "backtest", "report") else "live"
            if kind == "describe":
                parameter_path = args.output / "driver-parameters.json"
                private_write(parameter_path, action["parameters"])
                words = ["strategy", "describe", "--parameter-preset-id", action["parameter_preset_id"],
                         "--parameters", str(parameter_path)]
            elif kind in ("compile", "prepare", "backtest", "report"):
                references = {"compile": "parameter_version_id", "prepare": "dataset_id",
                              "backtest": "strategy_version_id", "report": "backtest_id"}
                reference = references[kind]
                words = ["strategy", kind, "--" + reference.replace("_", "-"), action[reference]]
                if kind == "backtest":
                    words += ["--dataset-id", action["dataset_id"]]
                    if action.get("stress"):
                        words += ["--stress"]
            elif kind == "deploy":
                words = ["deployment", "create", "--strategy-version-id", action["strategy_version_id"],
                         "--binding-id", action["binding_id"]]
            elif kind == "start":
                words = ["run", "start", "--deployment-id", action["deployment_id"]]
            elif kind in ("pause", "resume", "stop", "restore"):
                words = ["run", kind, action["run_id"], "--expected-run-generation", action["expected_run_generation"]]
                if kind == "restore":
                    words += ["--recovery-point-sha256", action["recovery_point_sha256"]]
            else:
                raise CheckFailure("fixture-driver-unsupported-action")
            words += ["--idempotency-key", body["idempotency_key"]]
        use_frontend = bool(args.client_binary) and not cancel and kind in ("start", "pause", "resume", "stop", "restore", "backtest", "report")
        compatibility_reason = "fixture-setup-or-low-level-action"
        if use_frontend and kind in ("pause", "resume", "stop", "restore"):
            with original(opener, request.Request(session["origin"] + "/api/v2/resources/" + action["run_id"]), timeout=15) as response:
                fixed = json.load(response)
            if (fixed["body"].get("run_generation") != action["expected_run_generation"]
                    or (kind == "restore" and fixed["body"].get("recovery_point_sha256") != action["recovery_point_sha256"])):
                use_frontend = False
                compatibility_reason = "explicit-wrong-generation-or-recovery-hash"
        if use_frontend:
            if kind == "start":
                words = ["strategy", "start", action["deployment_id"]]
            elif kind == "restore":
                words = ["strategy", "start", action["run_id"]]
            elif kind in ("pause", "resume", "stop"):
                words = ["strategy", kind, action["run_id"]]
            elif kind == "backtest":
                words = ["research", "backtest", action["strategy_version_id"], "--dataset", action["dataset_id"]]
                if action.get("stress"):
                    words += ["--stress"]
            else:
                words = ["research", "report", action["backtest_id"]]
            words += ["--detach", "--timeout", "60"]
            remembered = frontend_journals.get(body["idempotency_key"])
            if remembered:
                require(remembered["action"] == action, "frontend-fixture-original-key-input-changed")
                words += ["--journal", remembered["journal_id"]]
        counter += 1
        argv = [str(args.binary), "console", "--url", session["origin"], "--username", session["username"],
                "--password-env", "TITAN_CLI_DRIVER_PASSWORD", "--node", node, "--environment", environment,
                "--json", *words]
        child_env = {**os.environ, "TITAN_CLI_DRIVER_PASSWORD": session["password"]}
        if use_frontend:
            frontend_config = args.output / "frontend-client.toml"
            private_write(frontend_config, toml({"schema_version": 1, "default_profile": "fixture", "profiles": {
                "fixture": {"url": session["origin"], "username": session["username"],
                            "pass" + "word_env": "TITAN_CLI_DRIVER_PASSWORD", "node": node, "environment": environment}}}))
            for variable, directory in (("HOME", "frontend-home"), ("XDG_CONFIG_HOME", "frontend-config"),
                    ("XDG_STATE_HOME", "frontend-state"), ("XDG_CACHE_HOME", "frontend-cache"), ("XDG_DATA_HOME", "frontend-data")):
                target = args.output / directory
                target.mkdir(mode=0o700, exist_ok=True)
                child_env[variable] = str(target)
            for variable in ("TITAN_CONFIG", "TITAN_CLIENT_CONFIG", "TITAN_PROFILE"):
                child_env.pop(variable, None)
            argv = [str(args.client_binary), "--client-config", str(frontend_config), "--profile", "fixture",
                    "--node", node, "--environment", environment, "--json", *words]
        private_write(args.output / f"driver-{counter:03d}.command.json", {"argv": argv,
            "path": "new-frontend" if use_frontend else "compatibility",
            "compatibility_reason": None if use_frontend else compatibility_reason})
        result = subprocess.run(argv, capture_output=True, timeout=90, env=child_env)
        private_write(args.output / f"driver-{counter:03d}.result.json", {
            "exit_code": result.returncode, "stdout": result.stdout.decode(), "stderr": result.stderr.decode()})
        value = json.loads(result.stdout) if result.stdout.strip() else None
        frontend_outcome = (use_frontend and result.returncode == 7 and isinstance(value, dict)
            and value.get("kind") == "operation" and value.get("data", {}).get("state") in ("failed", "cancelled", "unknown"))
        if result.returncode and not frontend_outcome:
            status = {2: 400, 3: 403, 4: 404, 5: 409, 6: 503}.get(result.returncode, 500)
            raise error.HTTPError(resource.full_url, status, "CLI request rejected; private evidence recorded", {}, None)
        require(isinstance(value, dict) and value.get("schema_version") == 1 and value.get("api_version") == 2, "fixture-driver-envelope")
        data = value["data"]
        if use_frontend:
            journal_ids = [line.split("=", 1)[1] for line in result.stderr.decode().splitlines() if line.startswith("journal_id=")]
            journal_id = data.get("journal_id") if value.get("kind") == "research" else (journal_ids[-1] if journal_ids else None)
            require(isinstance(journal_id, str) and len(journal_id) == 64, "fixture-frontend-journal-identity")
            if value.get("kind") == "research":
                data = data["operation"]
            frontend_journals[body["idempotency_key"]] = {"action": action, "journal_id": journal_id,
                "operation_id": data["id"], "node": node, "environment": environment}
            private_write(args.output / "frontend-route-evidence.json", {
                "scope": "new strategy controls/research only; setup and explicit low-level negatives stay compatible",
                "mapped_original_fixture_keys": frontend_journals})
        return io.BytesIO(json.dumps(data).encode())

    previous_argv = sys.argv
    request.OpenerDirector.open = open_via_cli
    sys.argv = ["fixture", "--repo", str(args.repo), "--binary", str(args.binary), "--output", str(args.output)]
    if offline:
        sys.argv += ["--history", str(args.history_binary), "--data", str(args.data), "--parameters", str(args.parameters)]
    try:
        (management_console_smoke if offline else management_control_smoke).main()
    finally:
        request.OpenerDirector.open = original
        sys.argv = previous_argv


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "binary", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--client-binary", type=Path, help="Opt in to new frontend strategy controls/research backtest/report; setup and explicit low-level negatives stay compatible")
    parser.add_argument("--build", action="store_true", help="Run real SDK Describe/freeze/native compilation")
    parser.add_argument("--lifecycle", action="store_true", help="Reuse control smoke fixture with actual lifecycle CLI commands")
    parser.add_argument("--offline", action="store_true", help="Reuse historical smoke fixture with actual prepare/backtest/report commands")
    for name in ("history-binary", "data", "parameters"):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--control-driver", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--offline-driver", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if (args.offline or args.offline_driver) and not all((args.history_binary, args.data, args.parameters)):
        parser.error("--offline requires --history-binary, --data and --parameters")
    address = os.environ.get("TITAN_BUILD_ADDRESS", "")
    if not address or sys.platform != "linux":
        parser.error("run on configured titan-build with TITAN_BUILD_ADDRESS")
    host = subprocess.run(["hostname", "-I"], capture_output=True, text=True, check=True)
    if address not in host.stdout.split():
        parser.error("this host does not match the configured titan-build")
    args.repo, args.binary, args.output = args.repo.resolve(), args.binary.resolve(), args.output.resolve()
    if args.client_binary:
        args.client_binary = args.client_binary.resolve()
    ignored = subprocess.run(["git", "-C", str(args.repo), "check-ignore", "--quiet", str(args.output)], capture_output=True)
    if args.output.is_relative_to(args.repo) and ignored.returncode != 0:
        parser.error("--output inside the repository must be ignored private storage")
    if args.output.exists():
        parser.error("--output must be a new private directory")
    os.umask(0o077)
    if args.control_driver or args.offline_driver:
        fixture_driver(args, offline=args.offline_driver)
        return 0
    args.output.mkdir(parents=True, mode=0o700)
    matrix = Matrix(args)
    summary = {"schema_version": 1, "scope": "synthetic loopback; no real accounts or venues",
        "redaction": "allowlisted case names/status/counts/hashes only; full evidence private in --output",
        "binary_sha256": digest(args.binary), "script_sha256": digest(Path(__file__).resolve()),
        "passed": False, "checks": [], "skipped": [], "uncovered": [
            "venue/account runtime facts and financial precision", "SSE/reconnect/cursor expiration",
            "lifecycle deploy/start/pause/resume/stop/restore and risk release",
            "in-flight worker cancellation", "backtest/report dataset pipeline",
            "manager/node restart with accepted worker", "legacy CLI regression"]}
    failure = None
    try:
        matrix.setup()
        matrix.base()
        if args.build:
            matrix.build()
        else:
            summary["skipped"].append("Describe/freeze/compile requires --build and native SDK toolchain")
        if args.lifecycle:
            matrix.lifecycle()
            summary["uncovered"].remove("lifecycle deploy/start/pause/resume/stop/restore and risk release")
        else:
            summary["skipped"].append("CLI lifecycle fixture requires --lifecycle and SDK/native compiler/openssl")
        if args.offline:
            matrix.offline()
            summary["uncovered"].remove("backtest/report dataset pipeline")
        else:
            summary["skipped"].append("CLI history fixture requires --offline and synthetic historical input paths")
        if args.lifecycle or args.offline:
            summary["uncovered"].remove("manager/node restart with accepted worker")
        summary["passed"] = True
    except Exception as exc:
        import traceback
        private_write(args.output / "failure.txt", traceback.format_exc())
        failure = exc
        summary["failure"] = str(exc) if isinstance(exc, CheckFailure) else "private-evidence-required"
    finally:
        try:
            matrix.close()
        except Exception:
            summary["passed"] = False
            summary["cleanup"] = "failed-inspect-private-evidence"
        summary["checks"] = matrix.checks
        private_write(args.output / "summary.json", summary)
        print(json.dumps(summary), flush=True)
    return 0 if summary["passed"] and failure is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
