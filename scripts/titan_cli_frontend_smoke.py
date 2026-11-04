#!/usr/bin/env python3
"""Two real binaries, isolated frontend/config/bootstrap acceptance on titan-build.

python3 scripts/titan_cli_frontend_smoke.py --repo /workspace/titan \
  --server-binary /workspace/titan/target/debug/titan \
  --client-binary /workspace/titan/target/debug/titan_cli --output /private/frontend-smoke

No local execution. Only synthetic loopback management configuration is used.
Raw argv/stdout/stderr, hashes, config and process identities are private under
--output; stdout is reconstructed from fixed case names, status and hashes.
No shell/PTY test is duplicated here. Legacy management_cli_smoke.py remains an
independent regression gate; this script does not rewrite its prior results.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import sqlite3
import subprocess
import sys
import time
import tomllib
import traceback

from management_cli_smoke import Matrix as ManagementFixture
from management_cli_smoke import CheckFailure, digest, private_write, require, wait_for
from management_control_smoke import port, toml


def identity(pid):
    """Linux PID reuse guard; executable and argv are retained only privately."""
    root = Path("/proc") / str(pid)
    try:
        raw = (root / "stat").read_text()
        return {"pid": pid, "start_ticks": int(raw.rsplit(")", 1)[1].split()[19]),
                "exe": str((root / "exe").resolve(strict=True)),
                "argv": [part.decode() for part in (root / "cmdline").read_bytes().split(b"\0") if part]}
    except (OSError, ValueError, UnicodeError):
        return None


class Matrix(ManagementFixture):
    def __init__(self, args):
        args.binary = args.server_binary
        super().__init__(args)
        self.owned = {}
        self.current_case = "startup"
        self.daemon_configs = []
        self.birth = identity(os.getpid())["start_ticks"]
        # Isolate discovery, aliases, journal and bootstrap state from real users.
        for key in list(self.env):
            if key.startswith("TITAN_") or key.upper() in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"}:
                self.env.pop(key)
        self.env.update(TITAN_FRONTEND_PASSWORD=self.password,
                        TITAN_CLI_SMOKE_PASSWORD=self.password,
                        TITAN_CONSOLE_NODE_TOKEN=secrets.token_hex(32),
                        NO_PROXY="127.0.0.1,localhost", PYTHONDONTWRITEBYTECODE="1",
                        TITAN_STRATEGY_SDK=str(args.repo / "python/titan-strategy-sdk"),
                        TITAN_STRATEGY_PYTHON=os.environ.get("TITAN_STRATEGY_PYTHON", "python3"))
        for key, name in (("HOME", "home"), ("XDG_CONFIG_HOME", "xdg-config"),
                          ("XDG_DATA_HOME", "xdg-data"), ("XDG_STATE_HOME", "xdg-state"),
                          ("XDG_CACHE_HOME", "xdg-cache"), ("XDG_RUNTIME_DIR", "xdg-runtime")):
            target = self.root / name
            target.mkdir(mode=0o700)
            self.env[key] = str(target)

    def raw(self, name, argv, *, input=None, timeout=60, env=None):
        self.sequence += 1
        self.current_case = name
        stem = self.root / f"{self.sequence:03d}-{name}"
        private_write(stem.with_suffix(".command.json"), {"argv": list(map(str, argv))})
        with stem.with_suffix(".stdout").open("wb") as out, stem.with_suffix(".stderr").open("wb") as err:
            process = subprocess.Popen(list(map(str, argv)), env=env or self.env, cwd=self.args.repo,
                stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL, stdout=out, stderr=err)
            try:
                process.communicate(input=input, timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                private_write(stem.with_suffix(".result.json"), {"exit_code": process.returncode, "timed_out": True})
                raise CheckFailure(name + "-process-timeout") from None
        private_write(stem.with_suffix(".result.json"), {"exit_code": process.returncode})
        stdout, stderr = stem.with_suffix(".stdout").read_text(), stem.with_suffix(".stderr").read_text()
        require(self.password not in stdout + stderr, name + "-password-disclosure")
        return process.returncode, stdout, stderr

    def front(self, name, *words, code=0, profile="operator", config=None, machine=True, error_code=None, timeout=60, env=None):
        argv = [self.args.client_binary]
        if config is not False:
            argv += ["--client-config", config or self.root / "client.toml"]
        if profile is not None:
            argv += ["--profile", profile]
        if machine:
            argv += ["--json"]
        actual, stdout, stderr = self.raw(name, [*argv, *words], timeout=timeout, env=env)
        require(actual in code if isinstance(code, tuple) else actual == code, name + "-exit")
        if error_code and actual:
            errors = []
            for line in stderr.splitlines():
                try:
                    errors.append(json.loads(line))
                except ValueError:
                    continue
            require(any(value.get("data", {}).get("code") == error_code for value in errors), name + "-error-code")
        if not machine:
            return stdout
        if not stdout.strip():
            return None
        try:
            value = json.loads(stdout)
        except ValueError:
            raise CheckFailure(name + "-json") from None
        require(value.get("schema_version") == 1 and value.get("api_version") == 2, name + "-schema")
        return value["data"]

    def start(self, kind):
        config_path = self.root / "console.toml"
        if kind == "node":
            config = tomllib.loads(config_path.read_text())
            for preset in config["node"]["parameter_presets"].values():
                preset["strategy_name"] = "synthetic-duplicate"
            private_write(config_path, toml(config))
        log = (self.root / (kind + ".log")).open("ab")
        self.logs.append(log)
        process = subprocess.Popen([str(self.args.server_binary), "serve", kind, "--config", str(config_path)],
            env=self.env, cwd=self.args.repo, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        self.processes.append(process)
        proof = wait_for(lambda: identity(process.pid), timeout=5)
        require(proof["exe"] == str(self.args.server_binary), "fixture-server-executable")
        self.owned[process.pid] = proof
        return process

    def helps(self):
        for label, binary in (("server", self.args.server_binary), ("client", self.args.client_binary)):
            for flag in ("--help", "--version"):
                code, stdout, _ = self.raw(label + flag, [binary, flag], timeout=10)
                require(code == 0 and bool(stdout.strip()), label + flag + "-output")
        for role in ("manager", "node"):
            code, text, _ = self.raw("serve-" + role + "-help", [self.args.server_binary, "serve", role, "--help"], timeout=10)
            require(code == 0 and "--config" in text, "serve-role-help")
        for group, children in (("strategy", ("ls", "show", "start", "pause", "resume", "stop", "logs")),
                                ("account", ("ls", "show", "balance", "positions", "orders", "risk", "fills")),
                                ("research", ("build", "backtest", "report")),
                                ("config", ("database", "show", "check"))):
            for words in ((group,), (group, "--help"), ("help", group)):
                text = self.front("help-" + "-".join(words), *words, config=False, profile=None, machine=False, timeout=10)
                require(bool(text.strip()), "group-help-output")
            for child in children:
                text = self.front("help-" + group + "-" + child, group, child, "--help",
                                  config=False, profile=None, machine=False, timeout=10)
                require(bool(text.strip()), "subcommand-help-output")
        text = self.front("noninteractive-no-arguments", config=False, profile=None, machine=False, timeout=10)
        require("strategy" in text and "account" in text, "noninteractive-help-does-not-hang")
        require(not list((self.root / "xdg-config").rglob("*.pid")), "help-no-bootstrap-state")
        self.record("two-real-binaries-and-each-help-level-without-connection")

    def offline_configuration(self):
        path = self.root / "xdg-config/titan/client.toml"
        path.parent.mkdir(mode=0o700, exist_ok=True)
        document = {"schema_version": 1, "default_profile": "offline", "profiles": {
            "offline": {"url": f"http://127.0.0.1:{port()}", "username": "synthetic",
                        "pass" + "word_env": "TITAN_FRONTEND_PASSWORD"}, "second": {}}}
        private_write(path, toml(document))
        shown = self.front("offline-show", "config", "show", config=False, profile=None)
        require(shown["database"]["configured"] is False, "offline-show-no-daemon")
        self.front("select-default-profile", "use", "second", config=False, profile=None)
        require(tomllib.loads(path.read_text())["default_profile"] == "second", "use-persists-selection")
        self.front("select-offline-profile", "use", "offline", config=False, profile=None)
        require(path.stat().st_mode & 0o777 == 0o600, "profile-config-private-mode")
        storage = self.root / "offline-storage"
        saved = self.front("sqlite-save-only", "config", "database", "--history", "sqlite", "--storage-root", storage,
                           "--save-only", config=False, profile=None)
        require(not storage.exists(), "save-only-does-not-initialize-storage")
        initialized = self.front("sqlite-initialize", "config", "database", "--history", "sqlite", "--storage-root", storage,
                                 config=False, profile=None)
        require(storage.is_dir() and initialized["initialized"] is True, "sqlite-storage-initialized")
        before = {str(p.relative_to(storage)): digest(p) for p in storage.rglob("*.db")}
        require(bool(before), "sqlite-control-database-present")
        checked = self.front("offline-check", "config", "check", config=False, profile=None)
        require(checked["management_backend"] == checked["recovery_backend"] == "local_sqlite", "sqlite-control-and-recovery-local")
        require(before == {str(p.relative_to(storage)): digest(p) for p in storage.rglob("*.db")},
                "config-check-does-not-rewrite-storage")
        require(not any("runtime" in p.name or "checkpoint" in p.name for p in storage.rglob("*")),
                "configuration-does-not-create-recovery-database")
        shown = self.front("redacted-config-show", "config", "show", config=False, profile=None)
        serialized = json.dumps(shown)
        require(str(storage) not in serialized and document["profiles"]["offline"]["url"] not in serialized
                and "TITAN_FRONTEND_PASSWORD" not in serialized, "config-show-allowlist")
        self.front("sqlite-rejects-postgres-options", "config", "database", "--history", "sqlite", "--storage-root", storage,
                   "--dsn-env", "TITAN_SYNTHETIC_DSN", config=False, profile=None, code=2, error_code="INVALID_CONFIG")
        self.front("postgres-requires-reference", "config", "database", "--history", "postgres", "--storage-root", storage,
                   config=False, profile=None, code=2, error_code="INVALID_CONFIG")
        self.front("postgres-rejects-dsn-parameter", "config", "database", "--dsn", config=False, profile=None, code=2)
        self.front("postgres-rejects-invalid-reference", "config", "database", "--history", "postgres", "--storage-root", storage,
                   "--dsn-env", "invalid-reference-name", config=False, profile=None, code=2, error_code="INVALID_CONFIG")
        self.front("postgres-save-reference-candidate", "config", "database", "--history", "postgres", "--storage-root", storage,
                   "--dsn-env", "TITAN_FRONTEND_ABSENT_DSN", "--save-only", config=False, profile=None)
        configured = tomllib.loads(path.read_text())["profiles"]["offline"]["database"]
        require(configured["dsn_env"] == "TITAN_FRONTEND_ABSENT_DSN" and configured["state"] == "candidate",
                "postgres-saves-only-reference-candidate")
        self.front("postgres-missing-reference-check", "config", "check", config=False, profile=None,
                   code=6, error_code="STORAGE_UNAVAILABLE")
        require(before == {str(p.relative_to(storage)): digest(p) for p in storage.rglob("*.db")},
                "postgres-candidate-preserves-immutable-sqlite-store")
        shown = self.front("postgres-redacted-show", "config", "show", config=False, profile=None)
        require(shown["database"]["credential_reference_configured"] is True
                and "TITAN_FRONTEND_ABSENT_DSN" not in json.dumps(shown), "postgres-reference-not-disclosed")
        # Explicit HOME fallback uses a separate file, never the real user's HOME.
        fallback = self.root / "home/.config/titan/client.toml"
        fallback.parent.mkdir(parents=True, mode=0o700)
        private_write(fallback, toml({"schema_version": 1, "default_profile": "home", "profiles": {"home": {}}}))
        environment = {key: value for key, value in self.env.items() if key != "XDG_CONFIG_HOME"}
        self.front("home-config-fallback", "config", "show", config=False, profile=None, env=environment)
        require(not self.owned and not list(self.root.rglob(".titan-daemon")), "offline-config-no-daemon")
        self.record("isolated-profile-discovery-and-offline-sqlite-postgres-reference-negatives")

    def names(self):
        # Use actual account-directory writes for setup; no fabricated runtime facts.
        second = self.call("/api/v2/accounts", {"node_id": "build", "name": "synthetic-account", "exchange": "gate",
            "environment": "backtest", "credential_ref": "", "notes": "synthetic", "enabled": True,
            "expected_revision": None})
        require(second["id"] != self.account["id"], "distinct-account-identities")
        self.front("account-ls", "account", "ls")
        self.front("account-id", "account", "show", self.account["id"])
        self.front("account-name-ambiguous", "account", "show", "synthetic-account", code=2, error_code="AMBIGUOUS_TARGET")
        self.front("strategy-name-ambiguous", "strategy", "show", "synthetic-duplicate", code=2, error_code="AMBIGUOUS_TARGET")
        self.front("strategy-exact-id", "strategy", "show", "grid")
        config = tomllib.loads((self.root / "client.toml").read_text())
        config["profiles"]["operator"]["aliases"] = {"fixed-account": self.account["id"], "fixed-grid": "grid"}
        private_write(self.root / "client.toml", toml(config))
        require(self.front("account-fixed-alias", "account", "show", "fixed-account")["id"] == self.account["id"],
                "account-alias-fixes-exact-id")
        self.front("strategy-fixed-alias", "strategy", "show", "fixed-grid")
        require(self.front("account-missing-facts", "account", "balance", "fixed-account")["data"] is None,
                "frontend-missing-balance-stays-null")
        with sqlite3.connect(self.root / "manager.db") as db:
            require(db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0, "read-queries-no-strategy-start")
        text = self.front("default-human-account-list", "account", "ls", machine=False)
        require(bool(text.strip()) and not text.lstrip().startswith("{"), "human-list-output")
        self.record("readable-target-ambiguity-rejected-and-exact-id-preserved")

    def waiting_and_unknown(self):
        def operation_rows():
            with sqlite3.connect(self.root / "manager.db") as db:
                return [(ident, key, json.loads(data)) for ident, key, data in db.execute(
                    "SELECT id,key,data FROM operations ORDER BY rowid")]
        baseline = {row[0] for row in operation_rows()}
        self.pause()
        try:
            self.front("research-default-wait-timeout", "research", "build", "grid", "--timeout", "1",
                       code=8, error_code="WAIT_TIMEOUT", timeout=25)
            created = [row for row in operation_rows() if row[0] not in baseline]
            require(len(created) == 1, "default-wait-single-submission")
            ident, original_key, operation = created[0]
            require(operation["state"] == "pending" and not operation["cancel_requested"],
                    "default-wait-timeout-no-cancellation")
            # Fixture cleanup only: no execution has been admitted by the stopped node.
            self.call("/api/v2/operations/" + ident + "/cancel", {})
        finally:
            self.resume()
        wait_for(lambda: self.operation(ident)["state"] == "cancelled")
        self.record("research-waits-by-default-and-timeout-preserves-original-operation")
        baseline = {row[0] for row in operation_rows()}
        unknown_parameters = self.root / "unknown-parameters.json"
        private_write(unknown_parameters, {"synthetic_unknown_probe": 1})
        self.pause()
        try:
            detached = self.front("research-detached", "research", "build", "grid", "--parameters", unknown_parameters,
                                  "--detach", "--timeout", "60")
            require(detached["complete"] is False and detached["operation"]["state"] == "pending", "detach-only-accepted")
            journal_id = detached["journal_id"]
            require(len(journal_id) == 64 and all(c in "0123456789abcdef" for c in journal_id), "private-journal-identity")
            op = detached["operation"]
            directory = self.root / "node/operations" / op["id"]
            directory.mkdir()
            private_write(self.root / "frontend-fault.json", {"kind": "admission-evidence-without-receipt",
                          "operation_id": op["id"], "journal_id": journal_id})
        finally:
            self.resume()
        wait_for(lambda: self.operation(op["id"])["state"] == "unknown")
        self.front("research-unknown-journal", "research", "build", "--journal", journal_id,
                   "--timeout", "2", code=7, error_code="OPERATION_UNKNOWN")
        self.front("research-unknown-journal-repeat", "research", "build", "--journal", journal_id,
                   "--timeout", "2", code=7, error_code="OPERATION_UNKNOWN")
        self.front("research-unknown-same-input", "research", "build", "grid", "--parameters", unknown_parameters,
                   "--timeout", "1", code=7, error_code="OPERATION_UNKNOWN")
        created = [row for row in operation_rows() if row[0] not in baseline]
        require(len(created) == 1 and created[0][0] == op["id"] and created[0][2]["state"] == "unknown",
                "unknown-original-operation-no-new-submit")
        require(not (directory / "request.json").exists(), "unknown-no-worker-redispatch")
        journals = [path for path in self.root.rglob("*.jsonl") if journal_id in path.name]
        require(len(journals) == 1 and journals[0].stat().st_mode & 0o777 == 0o600, "journal-private-mode")
        entries = [json.loads(line) for line in journals[0].read_text().splitlines()]
        intents = [entry for entry in entries if entry.get("event") == "operation_intent"]
        require(len(intents) == 1 and intents[0]["key"] == created[0][1], "journal-retains-only-original-stage-key")
        require(entries.index(intents[0]) < next(index for index, entry in enumerate(entries)
            if entry.get("event") == "operation_receipt"), "journal-intent-before-receipt")
        self.record("detach-and-unknown-journal-never-resubmit-or-advance-pipeline")

    def terminal_failure_retry(self):
        parameters = self.root / "terminal-failure-parameters.json"
        private_write(parameters, {"unexpected": 1, "synthetic_terminal_failure_probe": 1})
        def count():
            with sqlite3.connect(self.root / "manager.db") as db:
                return db.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
        before = count()
        first = self.front("terminal-research-failure", "research", "build", "grid", "--parameters", parameters,
                           "--timeout", "60", code=7, error_code="OPERATION_FAILED", timeout=90)
        require(first["state"] == "failed", "research-terminal-failure-proven")
        matching = []
        for path in self.root.rglob("*.jsonl"):
            events = [json.loads(line) for line in path.read_text().splitlines()]
            if any(event.get("event") == "operation_receipt" and event.get("receipt", {}).get("id") == first["id"]
                   for event in events):
                matching.append((path, events))
        require(len(matching) == 1 and any(event.get("event") == "finished" and event.get("outcome") == "failed"
                    for event in matching[0][1]), "terminal-failed-journal-finished")
        # Same config/profile/target/input: no namespace change hides this rule.
        second = self.front("terminal-research-explicit-retry", "research", "build", "grid", "--parameters", parameters,
                            "--timeout", "60", code=7, error_code="OPERATION_FAILED", timeout=90)
        require(second["state"] == "failed" and second["id"] != first["id"] and count() == before + 2,
                "terminal-failure-explicit-retry-new-operation")
        original = self.front("terminal-failed-original-journal", "research", "build", "--journal", matching[0][0].stem,
                              "--timeout", "2", code=7, error_code="OPERATION_FAILED")
        require(original["id"] == first["id"] and count() == before + 2, "explicit-old-failed-journal-never-retries")
        self.record("proven-terminal-failure-allows-new-intent-and-old-journal-stays-fixed")

    def native_build(self):
        self.env.update(TITAN_STRATEGY_SDK=str(self.args.repo / "python/titan-strategy-sdk"),
                        TITAN_STRATEGY_PYTHON=os.environ.get("TITAN_STRATEGY_PYTHON", "python3"))
        code, stdout, _ = self.raw("native-parameter-fixture", [self.args.server_binary, "strategy", "describe",
            self.args.repo / "strategies/grid.py", "--json"], timeout=180)
        require(code == 0, "native-normalized-parameter-fixture")
        parameters = self.root / "native-parameters.json"
        private_write(parameters, json.loads(stdout)["normalized_parameters"])
        result = self.front("native-frontend-research-build", "research", "build", "grid", "--parameters", parameters,
                            "--timeout", "900", timeout=1900)
        require(result["complete"] is True and result["stage"] == "compile"
                and result["operation"]["state"] == "succeeded", "new-frontend-build-completed")
        version = result["resource"]
        require(version["kind"] == "strategy_version" and version["body"]["strategy_id"] == "grid",
                "new-frontend-version-business-identity")
        journal_id = result["journal_id"]
        journals = [path for path in self.root.rglob("*.jsonl") if journal_id in path.name]
        require(len(journals) == 1, "new-frontend-build-journal")
        events = [json.loads(line) for line in journals[0].read_text().splitlines()]
        intents = [item for item in events if item.get("event") == "operation_intent"]
        require({item["stage"] for item in intents} == {"describe", "compile"}, "new-frontend-build-stage-operations")
        saved = [item["resource"] for item in events if item.get("event") == "saved"]
        frozen = [item for item in saved if item["frozen"]]
        require(len(frozen) == 1 and frozen[0]["body"]["strategy_id"] == "grid", "new-frontend-frozen-parameter-version")
        require(version["body"].get("parameter_version_id") == frozen[0]["id"], "new-frontend-fixed-parameter-version")
        operation_id = result["operation"]["id"]
        bundle = self.root / "node/operations" / operation_id / "strategy.titan"
        require(bundle.is_file() and bundle.stat().st_size > 0, "new-frontend-native-bundle-produced")
        private_der, public_der = self.root / "native-signing.der", self.root / "native-public.der"
        private_der.write_bytes(bytes.fromhex("302e020100300506032b657004220420") + (self.root / "signing.key").read_bytes())
        private_der.chmod(0o600)
        code, _, _ = self.raw("native-public-key-fixture", ["openssl", "pkey", "-inform", "DER", "-in", private_der,
            "-pubout", "-outform", "DER", "-out", public_der])
        require(code == 0, "native-signing-public-key-fixture")
        public_key = public_der.read_bytes()[-32:].hex()
        code, validation, _ = self.raw("native-rust-signature-validation", [self.args.server_binary, "strategy", "validate",
            bundle, "--trusted-key", "synthetic=" + public_key, "--require-signature", "--json"])
        require(code == 0 and json.loads(validation)["valid"] is True
                and json.loads(validation)["compiled_strategy_digest"] == version["body"]["compiled_strategy_digest"],
                "new-frontend-bundle-signature-and-identity")
        require(digest(bundle) == version["body"]["strategy_file_sha256"], "new-frontend-published-file-identity")
        require(version == self.call("/api/v2/resources/" + version["id"]), "new-frontend-version-authoritative-comparison")
        private_write(self.root / "native-build-evidence.json", {"journal_id": journal_id, "strategy_version": version,
            "frozen_parameters": frozen[0], "bundle_sha256": digest(bundle), "operation_keys": [item["key"] for item in intents]})
        self.record("new-frontend-real-describe-save-freeze-native-compile")

    def candidates(self, config_path):
        result = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            proof = identity(int(entry.name))
            if (proof and proof["exe"] == str(self.args.server_binary) and "--config" in proof["argv"]
                    and str(config_path) in proof["argv"] and proof["start_ticks"] >= self.birth):
                result.append(proof)
        return result

    def remember_daemon(self, config_path):
        for proof in self.candidates(config_path):
            self.owned[proof["pid"]] = proof
        private_write(self.root / "owned-processes.json", list(self.owned.values()))

    def bootstrap(self):
        original = tomllib.loads((self.root / "console.toml").read_text())
        manager = original["manager"]
        endpoint = f"http://127.0.0.1:{port()}"
        manager.update(listen=endpoint.removeprefix("http://"), public_origin=endpoint,
                       database=str(self.root / "bootstrap-manager.db"))
        server_path = self.root / "bootstrap-server.toml"
        private_write(server_path, toml({"schema_version": 1, "manager": manager}))
        self.bootstrap_config = server_path
        self.daemon_configs.append(server_path)
        client_path = self.root / "bootstrap-client.toml"
        profile = {"url": endpoint, "username": "operator", "pass" + "word_env": "TITAN_FRONTEND_PASSWORD",
                   "node": "build", "environment": "backtest", "local_server": {
                       "manager_config": str(server_path), "program": str(self.args.server_binary), "startup_seconds": 15}}
        private_write(client_path, toml({"schema_version": 1, "default_profile": "local", "profiles": {"local": profile}}))
        processes = []
        try:
            for index in range(4):
                stem = self.root / f"bootstrap-concurrent-{index}"
                argv = [str(self.args.client_binary), "--client-config", str(client_path), "--json", "check"]
                out, err = stem.with_suffix(".stdout").open("wb"), stem.with_suffix(".stderr").open("wb")
                self.logs += [out, err]
                private_write(stem.with_suffix(".command.json"), {"argv": argv})
                processes.append(subprocess.Popen(argv, env=self.env, cwd=self.args.repo,
                    stdin=subprocess.DEVNULL, stdout=out, stderr=err))
            for index, process in enumerate(processes):
                actual = process.wait(timeout=35)
                private_write(self.root / f"bootstrap-concurrent-{index}.result.json", {"exit_code": actual})
                require(actual == 0, "parallel-bootstrap-check-exit")
                stdout = (self.root / f"bootstrap-concurrent-{index}.stdout").read_text()
                stderr = (self.root / f"bootstrap-concurrent-{index}.stderr").read_text()
                require(self.password not in stdout + stderr, "parallel-bootstrap-credential-disclosure")
                receipt = json.loads(stdout)
                require(receipt.get("schema_version") == 1 and receipt.get("api_version") == 2
                        and receipt.get("kind") == "connection", "parallel-bootstrap-connection-envelope")
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
            self.remember_daemon(server_path)
        actual = self.candidates(server_path)
        records = [json.loads(path.read_text()) for path in (self.root / ".titan-daemon").glob("*.json")]
        records = [record for record in records if record.get("manager_config") == str(server_path.resolve())]
        require(len(records) == 1 and len(actual) == 1, "bootstrap-single-private-identity-record")
        require(records[0]["pid"] == actual[0]["pid"] and records[0]["start_time"] == actual[0]["start_ticks"]
                and records[0]["boot_id"] == Path("/proc/sys/kernel/random/boot_id").read_text().strip()
                and records[0]["program"] == str(self.args.server_binary), "bootstrap-record-matches-running-process")
        require(all(path.stat().st_mode & 0o777 == 0o600 for path in (self.root / ".titan-daemon").glob("*.json")),
                "bootstrap-private-identity-permissions")
        require(len(actual) == 1 and "manager" in actual[0]["argv"] and "node" not in actual[0]["argv"],
                "parallel-bootstrap-single-manager")
        self.front("reuse-local-manager", "check", config=client_path, profile=None)
        require(self.candidates(server_path) == actual, "bootstrap-reuses-same-process-identity")
        with sqlite3.connect(self.root / "bootstrap-manager.db") as db:
            require(db.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0, "bootstrap-query-no-strategy-operation")
        require(not (self.root / "bootstrap-node").exists(), "bootstrap-no-execution-node")
        self.record("parallel-bootstrap-starts-one-manager-and-no-strategy")
        # A remote override must not start the configured local daemon at all.
        remote_server = self.root / "remote-never-start.toml"
        remote_cfg = dict(manager)
        remote_cfg["database"] = str(self.root / "remote-never.db")
        private_write(remote_server, toml({"schema_version": 1, "manager": remote_cfg}))
        self.daemon_configs.append(remote_server)
        remote_profile = {**profile, "url": "http://192.0.2.1:9", "local_server": {
            "manager_config": str(remote_server), "program": str(self.args.server_binary), "startup_seconds": 2}}
        remote_path = self.root / "remote-client.toml"
        private_write(remote_path, toml({"schema_version": 1, "default_profile": "remote", "profiles": {"remote": remote_profile}}))
        self.front("remote-no-local-fallback", "check", config=remote_path, profile=None, code=6, timeout=45)
        require(not self.candidates(remote_server) and not (self.root / "remote-never.db").exists(), "remote-failure-no-local-manager")
        self.record("remote-connection-failure-never-starts-local-manager")
        disabled_server = self.root / "disabled-server.toml"
        disabled_origin = f"http://127.0.0.1:{port()}"
        disabled_cfg = {**manager, "listen": disabled_origin.removeprefix("http://"),
                        "public_origin": disabled_origin, "database": str(self.root / "disabled-manager.db")}
        private_write(disabled_server, toml({"schema_version": 1, "manager": disabled_cfg}))
        self.daemon_configs.append(disabled_server)
        disabled_profile = {**profile, "url": disabled_origin, "local_server": {
            "manager_config": str(disabled_server), "program": str(self.args.server_binary), "startup_seconds": 2}}
        disabled_path = self.root / "disabled-client.toml"
        private_write(disabled_path, toml({"schema_version": 1, "default_profile": "disabled", "profiles": {"disabled": disabled_profile}}))
        self.front("explicit-no-auto-start", "--no-auto-start", "check", config=disabled_path, profile=None, code=6)
        require(not self.candidates(disabled_server) and not (self.root / "disabled-manager.db").exists(),
                "no-auto-start-preserves-stopped-state")
        self.record("explicit-no-auto-start-keeps-local-manager-stopped")

    def service_identity(self):
        original = tomllib.loads((self.root / "console.toml").read_text())
        manager = {**original["manager"], "database": str(self.root / "identity-wrong.db")}
        wrong = self.root / "identity-wrong-server.toml"
        private_write(wrong, toml({"schema_version": 1, "manager": manager}))
        self.daemon_configs.append(wrong)
        client = self.root / "identity-client.toml"
        base = {"url": self.origin, "username": "operator", "pass" + "word_env": "TITAN_FRONTEND_PASSWORD",
                "node": "build", "environment": "backtest"}
        profile = {**base, "local_server": {"manager_config": str(wrong), "program": str(self.args.server_binary)}}
        private_write(client, toml({"schema_version": 1, "default_profile": "identity", "profiles": {"identity": profile}}))
        before = identity(self.manager.pid)
        for extra in ((), ("--no-auto-start",)):
            self.front("wrong-service-identity" + ("-no-auto" if extra else ""), *extra, "check",
                       config=client, profile=None, code=6, error_code="SERVICE_IDENTITY_MISMATCH")
        require(identity(self.manager.pid) == before and not self.candidates(wrong)
                and not (self.root / "identity-wrong.db").exists(), "identity-mismatch-retains-existing-manager")
        profile["local_server"]["manager_config"] = str(self.root / "console.toml")
        private_write(client, toml({"schema_version": 1, "default_profile": "identity", "profiles": {"identity": profile}}))
        self.front("manual-serve-matching-identity", "check", config=client, profile=None)
        self.front("manual-serve-matching-identity-no-auto", "--no-auto-start", "check", config=client, profile=None)
        require(identity(self.manager.pid) == before, "matching-service-identity-no-restart")
        self.record("service-identity-checks-existing-listener-without-restart")

    def missing_manager_database(self):
        server_path = self.bootstrap_config.resolve()
        active = self.candidates(server_path)
        require(len(active) == 1, "missing-db-own-bootstrap-manager")
        proof = active[0]
        require(self.owned.get(proof["pid"]) == proof and identity(proof["pid"]) == proof,
                "missing-db-manager-process-identity")
        os.kill(proof["pid"], signal.SIGINT)
        wait_for(lambda: identity(proof["pid"]) is None, timeout=15)
        require(not self.candidates(server_path), "missing-db-no-live-manager-before-move")
        root = self.root.resolve()
        configured = Path(tomllib.loads(server_path.read_text())["manager"]["database"])
        database = configured.resolve()
        backup = root / "bootstrap-manager.db.c10-backup"
        require(configured.is_absolute() and database.parent == root and database.name == "bootstrap-manager.db"
                and database.is_file() and not configured.is_symlink(), "missing-db-verified-absolute-source")
        require(backup.is_absolute() and backup.resolve().parent == root and not backup.exists(),
                "missing-db-verified-absolute-backup")
        records = [json.loads(path.read_text()) for path in (root / ".titan-daemon").glob("*.json")]
        require(any(record.get("manager_config") == str(server_path) and record.get("pid") == proof["pid"]
                    for record in records), "missing-db-retained-process-record")
        original_hash = digest(database)
        database.rename(backup)
        try:
            self.front("recorded-manager-missing-database", "check", config=root / "bootstrap-client.toml", profile=None,
                       code=6, error_code="LOCAL_SERVER_UNAVAILABLE")
            require(not self.candidates(server_path) and not database.exists(), "missing-db-no-spawn-or-empty-replacement")
        finally:
            # Even a failing implementation may only leave processes owned by this
            # new private config. Stop those verified identities before restoring.
            self.remember_daemon(server_path)
            for candidate in self.candidates(server_path):
                require(self.owned.get(candidate["pid"]) == candidate and identity(candidate["pid"]) == candidate,
                        "missing-db-recovery-process-identity")
                os.kill(candidate["pid"], signal.SIGINT)
                deadline = time.monotonic() + 12
                while identity(candidate["pid"]) == candidate and time.monotonic() < deadline:
                    time.sleep(0.1)
                if identity(candidate["pid"]) == candidate:
                    os.kill(candidate["pid"], signal.SIGKILL)
                    wait_for(lambda: identity(candidate["pid"]) is None, timeout=5)
            require(backup.resolve().parent == root and backup.is_file() and not backup.is_symlink(),
                    "missing-db-recovery-backup-identity")
            if database.exists():
                unexpected = root / "bootstrap-manager.db.unexpected-new"
                require(database.resolve().parent == root and not database.is_symlink()
                        and unexpected.is_absolute() and unexpected.resolve().parent == root and not unexpected.exists(),
                        "missing-db-recovery-replacement-path")
                database.rename(unexpected)
            require(database.is_absolute() and database.parent == root and not database.exists(),
                    "missing-db-recovery-original-path")
            backup.rename(database)
        require(digest(database) == original_hash and not backup.exists(), "missing-db-original-database-restored")
        self.record("recorded-manager-missing-database-refuses-empty-reinitialization")

    def legacy_compatibility(self):
        legacy_output = self.root / "legacy"
        self.daemon_configs.append(legacy_output / "console.toml")
        environment = {**self.env, "TITAN_BUILD_ADDRESS": self.args.build_address}
        code, _, _ = self.raw("legacy-real-cli-regression", [sys.executable,
            self.args.repo / "scripts/management_cli_smoke.py", "--repo", self.args.repo,
            "--binary", self.args.server_binary, "--output", legacy_output], env=environment, timeout=300)
        require(code == 0 and json.loads((legacy_output / "summary.json").read_text())["passed"],
                "legacy-real-management-cli-regression")
        self.record("unchanged-legacy-management-smoke-new-private-run")

    def close(self):
        for config in self.daemon_configs:
            self.remember_daemon(config)
        private_write(self.root / "cleanup-identities.json", list(self.owned.values()))
        for pid, proof in reversed(list(self.owned.items())):
            current = identity(pid)
            if current != proof:
                require(current is None, "cleanup-refuses-changed-process-identity")
                continue
            require(proof["start_ticks"] >= self.birth and proof["exe"] == str(self.args.server_binary),
                    "cleanup-owned-binary-and-start-time")
            require(any(str(self.root) in word for word in proof["argv"]), "cleanup-owned-config-root")
            os.kill(pid, signal.SIGINT)
            deadline = time.monotonic() + 12
            while identity(pid) == proof and time.monotonic() < deadline:
                time.sleep(0.1)
            if identity(pid) == proof:
                os.kill(pid, signal.SIGKILL)
        for process in self.processes:
            if process.poll() is None:
                process.wait(timeout=5)
        for log in self.logs:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("repo", "server-binary", "client-binary", "output"):
        parser.add_argument("--" + flag, type=Path, required=True)
    parser.add_argument("--native", action="store_true", help="Run successful new-frontend Describe/save/freeze/compile using the server SDK/native toolchain")
    parser.add_argument("--skip-research", action="store_true", help="Report research waiting/journal cases as skipped; run help/config/bootstrap/compatibility only")
    args = parser.parse_args()
    address = os.environ.get("TITAN_BUILD_ADDRESS", "")
    args.build_address = address
    if not address or sys.platform != "linux":
        parser.error("run on configured titan-build with TITAN_BUILD_ADDRESS")
    host = subprocess.run(["hostname", "-I"], capture_output=True, text=True, check=True)
    if address not in host.stdout.split():
        parser.error("host does not match configured titan-build")
    args.repo, args.server_binary, args.client_binary, args.output = (
        args.repo.resolve(), args.server_binary.resolve(), args.client_binary.resolve(), args.output.resolve())
    if args.output.exists():
        parser.error("--output must be a new private directory")
    if args.output.is_relative_to(args.repo):
        ignored = subprocess.run(["git", "-C", str(args.repo), "check-ignore", "--quiet", str(args.output)], capture_output=True)
        if ignored.returncode != 0:
            parser.error("repository output must be ignored private storage")
    os.umask(0o077)
    args.output.mkdir(parents=True, mode=0o700)
    matrix = Matrix(args)
    summary = {"schema_version": 1, "scope": "synthetic loopback; two real binaries; isolated HOME/XDG",
        "redaction": "fixed cases/status/hashes only; raw evidence private in --output", "passed": False,
        "server_binary_sha256": digest(args.server_binary), "client_binary_sha256": digest(args.client_binary),
        "script_sha256": digest(Path(__file__).resolve()), "checks": [], "skipped": [],
        "uncovered": ["PTY shell (separate smoke)", "real venue/account operations",
                      "legacy build/lifecycle/history beyond basic compatibility gate"]}
    try:
        matrix.helps()
        matrix.offline_configuration()
        matrix.setup()
        matrix.names()
        matrix.current_case = "bootstrap"
        matrix.bootstrap()
        matrix.service_identity()
        matrix.missing_manager_database()
        if args.skip_research:
            summary["skipped"].append("research-default-wait-detach-and-unknown-journal")
        else:
            matrix.current_case = "research"
            matrix.waiting_and_unknown()
        if args.native:
            matrix.terminal_failure_retry()
            matrix.native_build()
        else:
            summary["skipped"].append("new-frontend-successful-native-build-requires---native")
        matrix.legacy_compatibility()
        summary["passed"] = True
    except Exception as exc:
        private_write(args.output / "failure.txt", traceback.format_exc())
        summary["failure"] = str(exc) if isinstance(exc, CheckFailure) else matrix.current_case + "-unexpected-failure"
    finally:
        try:
            matrix.close()
        except Exception:
            private_write(args.output / "cleanup-failure.txt", traceback.format_exc())
            summary["passed"] = False
            summary["cleanup"] = "identity-checked-cleanup-failed"
        summary["checks"] = matrix.checks
        private_write(args.output / "summary.json", summary)
        print(json.dumps(summary), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
