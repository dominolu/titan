#!/usr/bin/env python3
"""Real titan_cli PTY shell matrix; execute only on the configured titan-build.

python3 scripts/titan_cli_shell_smoke.py --repo /workspace/titan \
  --binary /workspace/titan/target/debug/titan_cli --output /private/shell-smoke-new

The HTTP fixture supplies synthetic directory records only. It records routes,
not credentials, and rejects lifecycle writes. Full PTY transcripts remain in a
new private output directory; stdout contains only allowlisted case summaries.
"""
from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import pty
import select
import struct
import subprocess
import sys
import termios
import threading
import time
import tomllib
import traceback
from urllib.parse import urlsplit


class CheckFailure(Exception):
    pass


def require(condition, name):
    if not condition:
        raise CheckFailure(name)


def private_write(path, value):
    path.write_text(value if isinstance(value, str) else json.dumps(value, indent=2))
    path.chmod(0o600)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class DirectoryFixture:
    def __init__(self):
        self.requests = []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def respond(self, code, value, cookie=False):
                body = json.dumps(value).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                if cookie:
                    self.send_header("Set-Cookie", "titan-session=fixture; HttpOnly; Path=/")
                self.end_headers()
                self.wfile.write(body)
                self.close_connection = True

            def do_POST(self):
                path = urlsplit(self.path).path
                fixture.requests.append({"method": "POST", "path": path})
                length = int(self.headers.get("Content-Length", "0"))
                if 0 < length <= 4096:
                    self.rfile.read(length)  # Never retain the login body.
                if path == "/api/v2/session":
                    self.respond(200, {"api_version": 2, "csrf_token": "fixture-csrf"}, cookie=True)
                else:
                    self.respond(405, {"code": "FIXTURE_WRITE_REJECTED"})

            def do_GET(self):
                path = urlsplit(self.path).path
                fixture.requests.append({"method": "GET", "path": path})
                if path in ("/api/v2/resources/target-one", "/api/v2/resources/explicit-two"):
                    identity = path.rsplit("/", 1)[-1]
                    self.respond(200, {"id": identity, "kind": "account_profile", "node_id": "fixture-node",
                        "revision": "1", "digest": "fixture-digest", "frozen": False,
                        "body": {"environment": "live", "name": identity, "connection_state": "not_connected"}})
                else:
                    self.respond(404, {"code": "NOT_FOUND"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class Terminal:
    def __init__(self, args, env, name, extra=()):
        self.args, self.name = args, name
        self.master, self.slave = pty.openpty()
        fcntl.ioctl(self.slave, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 120, 0, 0))
        self.original = termios.tcgetattr(self.slave)
        self.transcript = bytearray()
        self.position = 0
        argv = [str(args.binary), "--client-config", str(args.output / "client.toml"), *extra]
        self.process = subprocess.Popen(argv, stdin=self.slave, stdout=self.slave, stderr=self.slave,
                                        env=env, cwd=args.repo, start_new_session=True)

    def send(self, value):
        payload = value.encode() if isinstance(value, str) else value
        position = 0
        while position < len(payload):
            position += os.write(self.master, payload[position:])

    def expect(self, token, timeout=15):
        token = token.encode() if isinstance(token, str) else token
        deadline = time.monotonic() + timeout
        while True:
            # Every new readline enables bracketed paste exactly once. Typing
            # redraws the same prompt without that marker, so a plain prompt
            # match could race the command still executing after Enter.
            start = self.position
            if token.startswith(b"titan["):
                marker = self.transcript.find(b"\x1b[?2004h", start)
                start = marker + len(b"\x1b[?2004h") if marker >= 0 else len(self.transcript)
            found = self.transcript.find(token, start)
            if found >= 0:
                self.position = found + len(token)
                return
            require(time.monotonic() < deadline, self.name + "-prompt-timeout")
            readable, _, _ = select.select([self.master], [], [], 0.1)
            if readable:
                try:
                    chunk = os.read(self.master, 65_536)
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
                    chunk = b""
                if chunk:
                    self.transcript.extend(chunk)
                    require(len(self.transcript) <= 4 * 1024 * 1024, self.name + "-transcript-budget")
                    continue
            require(self.process.poll() is None, self.name + "-early-exit")

    def finish(self, command=b"exit\r"):
        self.send(command)
        deadline = time.monotonic() + 15
        while self.process.poll() is None:
            require(time.monotonic() < deadline, self.name + "-exit-timeout")
            readable, _, _ = select.select([self.master], [], [], 0.05)
            if readable:
                try:
                    self.transcript.extend(os.read(self.master, 65_536))
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
        require(self.process.wait(timeout=5) == 0, self.name + "-exit-code")
        require(termios.tcgetattr(self.slave) == self.original, self.name + "-terminal-restored")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        private_write(self.args.output / (self.name + ".transcript"), self.transcript.decode(errors="replace"))
        os.close(self.master)
        os.close(self.slave)


def execute(args, summary):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("TITAN_") and key.upper() not in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"}}
    env.update(HOME=str(args.output / "home"), XDG_CONFIG_HOME=str(args.output / "config"),
               XDG_STATE_HOME=str(args.output / "state"), TITAN_SHELL_FIXTURE_PASSWORD="fixture-only",
               TITAN_NO_AUTO_START="1", NO_PROXY="127.0.0.1,localhost", PYTHONDONTWRITEBYTECODE="1")
    with DirectoryFixture() as fixture:
        profiles = ['schema_version = 1', 'default_profile = "alpha"']
        for name in ("alpha", "beta"):
            profiles.extend([f"[profiles.{name}]", f"url = {json.dumps(fixture.origin)}", 'username = "fixture-reader"',
                             'password_env = "TITAN_SHELL_FIXTURE_PASSWORD"', 'node = "fixture-node"', 'environment = "live"'])
        private_write(args.output / "client.toml", "\n".join(profiles) + "\n")
        terminal = Terminal(args, env, "root-shell")
        try:
            terminal.expect("titan[alpha/root/-]> ")
            terminal.send("help\r")
            terminal.expect("Shell:")
            terminal.expect("titan[alpha/root/-]> ")
            terminal.send("str\t\r")
            terminal.expect("titan[alpha/strategy/-]> ")
            terminal.send("help\r")
            terminal.expect("Shell:")
            terminal.expect("titan[alpha/strategy/-]> ")
            terminal.send("show\r")  # Missing target must remain a local help request.
            terminal.expect("titan[alpha/strategy/-]> ")
            terminal.send("discard-this-line\x03")
            terminal.expect("^C")
            terminal.expect("titan[alpha/strategy/-]> ")
            require(not fixture.requests, "help-completion-groups-ctrlc-no-auth")
            summary["checks"].append({"name": "tty-help-tab-groups-and-ctrlc-without-auth", "status": "passed"})

            terminal.send("back\r")
            terminal.expect("titan[alpha/root/-]> ")
            terminal.send("account\r")
            terminal.expect("titan[alpha/account/-]> ")
            terminal.send("use target-one\r")
            terminal.expect("titan[alpha/account/target-one]> ")
            require(not fixture.requests, "use-target-local-only")
            terminal.send("show\r")
            terminal.expect("titan[alpha/account/target-one]> ")
            require(any(item["path"] == "/api/v2/resources/target-one" for item in fixture.requests), "context-target-dispatched")
            terminal.send("show explicit-two\r")
            terminal.expect("titan[alpha/account/target-one]> ")
            require(any(item["path"] == "/api/v2/resources/explicit-two" for item in fixture.requests), "explicit-target-overrides-context")
            summary["checks"].append({"name": "account-context-and-explicit-target-share-dispatch", "status": "passed"})

            terminal.send("help\r")
            terminal.expect("Shell:")
            terminal.expect("titan[alpha/account/target-one]> ")
            terminal.send(b"\x1b[A\r")
            terminal.expect("Shell:")
            terminal.expect("titan[alpha/account/target-one]> ")
            terminal.send(b"\x1b[200~help\n\x1b[201~")
            terminal.send("\r")
            terminal.expect("Shell:")
            terminal.expect("titan[alpha/account/target-one]> ")
            count = len(fixture.requests)
            terminal.send("back\r")
            terminal.expect("titan[alpha/root/-]> ")
            terminal.send("use beta\r")
            terminal.expect("titan[beta/root/-]> ")
            require(len(fixture.requests) == count, "profile-selection-offline")
            terminal.send("account\r")
            terminal.expect("titan[beta/account/-]> ")
            summary["checks"].append({"name": "history-paste-profile-selection-clears-context", "status": "passed"})

            terminal.send("config database --dsn postgres://fixture-history-secret\r")
            terminal.expect("titan[beta/account/-]> ")
            terminal.send("not-a-command credential-fixture-marker\r")
            terminal.expect("titan[beta/account/-]> ")
            history_before_cap = (args.output / "state/titan/cli/history.json").read_text()
            require("fixture-history-secret" not in history_before_cap and "credential-fixture-marker" not in history_before_cap, "sensitive-history-not-recorded-before-eviction")
            terminal.send("".join(f"use cap{index:03}\r" for index in range(105)))
            terminal.expect("titan[beta/account/cap104]> ", timeout=45)
            terminal.finish()
        finally:
            terminal.close()
        require(tomllib.loads((args.output / "client.toml").read_text())["default_profile"] == "beta", "profile-selection-persisted")
        history = args.output / "state/titan/cli/history.json"
        lines = json.loads(history.read_text())
        require(len(lines) == 100 and all(len(line.encode()) <= 4096 for line in lines), "history-capacity")
        require(history.stat().st_mode & 0o777 == 0o600 and history.parent.stat().st_mode & 0o777 == 0o700, "history-private-mode")
        require("fixture-history-secret" not in history.read_text() and "credential-fixture-marker" not in history.read_text(), "history-rejects-sensitive-inputs")
        summary["checks"].append({"name": "bounded-private-history-and-terminal-restoration", "status": "passed"})

        terminal = Terminal(args, env, "group-eof", ("strategy",))
        try:
            terminal.expect("titan[beta/strategy/-]> ")
            terminal.finish(b"\x04")
        finally:
            terminal.close()
        summary["checks"].append({"name": "direct-group-eof-restores-terminal", "status": "passed"})

        before = len(fixture.requests)
        for name, extra, code in [("root-nontty", [], 0), ("group-nontty", ["account"], 0),
                                  ("json-help-nontty", ["--json", "--help"], 0), ("json-nontty", ["--json"], 2)]:
            result = subprocess.run([str(args.binary), "--client-config", str(args.output / "client.toml"), *extra],
                input=b"check\n", stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, env=env, cwd=args.repo)
            private_write(args.output / (name + ".stdout"), result.stdout.decode(errors="replace"))
            private_write(args.output / (name + ".stderr"), result.stderr.decode(errors="replace"))
            require(result.returncode == code, name + "-exit")
            if code == 0:
                require(bool(result.stdout), name + "-explicit-help")
            else:
                require(not result.stdout, name + "-no-nonjson-stdout")
                envelope = json.loads(result.stderr)
                require(envelope.get("schema_version") == 1 and envelope.get("api_version") == 2
                        and envelope.get("kind") == "error" and envelope.get("data", {}).get("code") == "INVALID_INPUT", name + "-structured-error")
        require(len(fixture.requests) == before, "noninteractive-help-does-not-authenticate")
        require(all(item["method"] == "GET" or item["path"] == "/api/v2/session" for item in fixture.requests), "no-lifecycle-writes")
        private_write(args.output / "requests.json", fixture.requests)
        summary["checks"].append({"name": "redirected-input-help-exits-without-reading-or-auth", "status": "passed"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "binary", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    address = os.environ.get("TITAN_BUILD_ADDRESS", "")
    if not address or sys.platform != "linux":
        parser.error("run on configured titan-build with TITAN_BUILD_ADDRESS")
    host = subprocess.run(["hostname", "-I"], capture_output=True, text=True, check=True)
    if address not in host.stdout.split():
        parser.error("this host does not match the configured titan-build")
    args.repo, args.binary, args.output = args.repo.resolve(), args.binary.resolve(), args.output.resolve()
    ignored = subprocess.run(["git", "-C", str(args.repo), "check-ignore", "--quiet", str(args.output)], capture_output=True)
    if args.output.is_relative_to(args.repo) and ignored.returncode != 0:
        parser.error("--output inside the repository must be ignored private storage")
    if args.output.exists() or not args.binary.is_file():
        parser.error("use a new private output directory and an existing server-built binary")
    os.umask(0o077)
    args.output.mkdir(parents=True, mode=0o700)
    summary = {"schema_version": 1, "scope": "real client PTY; synthetic read-only directory fixture",
        "redaction": "allowlisted names/status/hashes only; transcripts and raw requests private",
        "binary_sha256": digest(args.binary), "script_sha256": digest(Path(__file__).resolve()),
        "passed": False, "checks": [], "uncovered": ["real manager authorization", "lifecycle completion", "daemon startup", "database migration"]}
    try:
        execute(args, summary)
        summary["passed"] = True
    except Exception as error:
        private_write(args.output / "failure.txt", traceback.format_exc())
        summary["failure"] = str(error) if isinstance(error, CheckFailure) else "private-evidence-required"
    finally:
        private_write(args.output / "summary.json", summary)
        print(json.dumps(summary), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
