#!/usr/bin/env python3
"""Scripted loopback protocol faults driving the real managed CLI on titan-build.

Run only on the configured build host:
  python3 scripts/management_cli_watch_smoke.py --repo /workspace/titan \
    --binary /workspace/titan/target/debug/titan --output /private/watch-smoke

This fixture models HTTP observations and fixed worker.log pages only. It does
not run a manager, engine, ledger, connector, account, venue or lifecycle action.
Raw process output and request traces stay in a new private --output directory.
Stdout contains an allowlisted summary, never raw CLI output or connection data.
"""
from __future__ import annotations

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import threading
import time
import traceback
from urllib.parse import parse_qs, urlsplit


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


def observation(revision="10", generation="1", marker="current"):
    return {"id": "run-fixture", "kind": "run", "node_id": "fixture-node",
            "revision": revision, "digest": "fixture-digest", "frozen": False,
            "body": {"environment": "live", "worker_id": "fixture-worker",
                     "run_generation": generation, "available": True,
                     "observation_stale": False, "marker": marker}}


def snapshot(epoch, sequence, generation="1", marker="current"):
    return {"api_version": 2, "cursor": f"{epoch}:{sequence}",
            "server_time_ns": str(sequence), "operations": [],
            "resources": [observation(str(sequence), generation, marker)]}


class Fixture:
    def __init__(self, scenario, password):
        self.scenario, self.password = scenario, password
        self.requests, self.unexpected = [], []
        self.observation_reads, self.resource_reads, self.log_reads = 0, 0, 0
        self.lock = threading.Lock()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                pass

            def send_value(self, status, value, cookie=False):
                payload = json.dumps(value, separators=(",", ":")).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Connection", "close")
                if cookie:
                    self.send_header("Set-Cookie", "titan-session=fixture; HttpOnly; Path=/")
                self.end_headers()
                try:
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                self.close_connection = True

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                if self.path != "/api/v2/session" or not 0 < length <= 4096:
                    with fixture.lock:
                        fixture.unexpected.append("write-route")
                    self.send_value(405, {"code": "FIXTURE_WRITE_REJECTED"})
                    return
                try:
                    body = json.loads(self.rfile.read(length))
                except ValueError:
                    body = {}
                if body != {"username": "fixture-reader", "password": fixture.password}:
                    self.send_value(401, {"code": "UNAUTHENTICATED"})
                    return
                # Do not retain the login body or password in the request trace.
                with fixture.lock:
                    fixture.requests.append({"method": "POST", "path": self.path})
                self.send_value(200, {"api_version": 2, "csrf_token": "fixture-csrf"}, cookie=True)

            def do_GET(self):
                target = urlsplit(self.path)
                query = parse_qs(target.query, keep_blank_values=True)
                with fixture.lock:
                    fixture.requests.append({"method": "GET", "path": target.path, "query": query})
                if self.headers.get("Cookie") != "titan-session=fixture":
                    self.send_value(401, {"code": "UNAUTHENTICATED"})
                    return
                status, body = fixture.respond(target.path, query)
                self.send_value(status, body)

            def do_PATCH(self):
                with fixture.lock:
                    fixture.unexpected.append("write-route")
                self.send_value(405, {"code": "FIXTURE_WRITE_REJECTED"})

            do_DELETE = do_PATCH
            do_PUT = do_PATCH

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.origin = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive(), "fixture-server-cleanup")

    def respond(self, path, query):
        with self.lock:
            if path == "/api/v2/snapshot":
                self.observation_reads += 1
                index = self.observation_reads - 1
                if self.scenario == "reconnect":
                    responses = [(200, snapshot("epoch-a", 10, marker="accepted-10")),
                                 (200, snapshot("epoch-a", 9, marker="discard-this-marker")),
                                 (503, {"code": "FIXTURE_UNAVAILABLE"}),
                                 (200, snapshot("epoch-b", 1, marker="resynchronized")),
                                 (200, snapshot("epoch-b", 2, "2", "new-generation"))]
                    if index < len(responses):
                        return responses[index]
                elif self.scenario in ("forbidden", "not-found"):
                    if index == 0:
                        return 200, snapshot("epoch-a", 10)
                    status = 403 if self.scenario == "forbidden" else 404
                    return status, {"code": "FORBIDDEN" if status == 403 else "NOT_FOUND"}
                elif self.scenario == "interrupt":
                    return 200, snapshot("epoch-a", 10 + index)
            elif path == "/api/v2/resources/run-fixture":
                self.resource_reads += 1
                if self.scenario == "resource-revisions":
                    versions = [("10", "1"), ("9", "1"), ("11", "2")]
                    index = self.resource_reads - 1
                    if index < len(versions):
                        return 200, observation(*versions[index])
                elif self.scenario in ("resource-forbidden", "resource-not-found"):
                    if self.resource_reads == 1:
                        return 200, observation()
                    status = 403 if self.scenario == "resource-forbidden" else 404
                    return status, {"code": "FORBIDDEN" if status == 403 else "NOT_FOUND"}
                elif self.scenario == "log-source-change":
                    return 200, observation(generation="2" if self.resource_reads >= 3 else "1")
                else:
                    return 200, observation()
            elif path == "/api/v2/resources/run-fixture/log":
                self.log_reads += 1
                offset = int(query.get("offset", ["0"])[0])
                if query.get("generation") != ["1"]:
                    return 409, {"code": "STALE_GENERATION"}
                size = 65_536
                return 200, {"worker_id": "fixture-worker", "offset": str(offset),
                             "next_offset": str(offset + size), "size": str(131_072),
                             "more": offset + size < 131_072, "text": "x" * size}
            self.unexpected.append("unexpected-read-route-or-budget")
            return 404, {"code": "FIXTURE_UNEXPECTED_REQUEST"}


class Matrix:
    def __init__(self, args):
        self.args, self.checks = args, []
        self.password = secrets.token_hex(24)
        # Discard all inherited Titan connection/configuration and proxy settings.
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith("TITAN_") and key.upper() not in
                    {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"}}
        self.env.update(TITAN_WATCH_FIXTURE_PASSWORD=self.password,
                        NO_PROXY="127.0.0.1,localhost", PYTHONDONTWRITEBYTECODE="1")

    def run(self, name, scenario, words, code=0, interrupt=False):
        with Fixture(scenario, self.password) as fixture:
            argv = [str(self.args.binary), "console", "--url", fixture.origin,
                    "--username", "fixture-reader", "--password-env", "TITAN_WATCH_FIXTURE_PASSWORD",
                    "--ndjson", *words]
            stem = self.args.output / name
            out_path, err_path = stem.with_suffix(".stdout"), stem.with_suffix(".stderr")
            with out_path.open("wb") as stdout, err_path.open("wb") as stderr:
                process = subprocess.Popen(argv, env=self.env, stdin=subprocess.DEVNULL,
                                           stdout=stdout, stderr=stderr, cwd=self.args.repo)
                try:
                    if interrupt:
                        deadline = time.monotonic() + 15
                        while out_path.stat().st_size == 0:
                            require(process.poll() is None, name + "-early-exit")
                            require(time.monotonic() < deadline, name + "-first-frame-timeout")
                            time.sleep(0.02)
                        process.send_signal(signal.SIGINT)
                    actual = process.wait(timeout=25)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
            require(out_path.stat().st_size <= 2 * 1024 * 1024, name + "-stdout-budget")
            require(err_path.stat().st_size <= 128 * 1024, name + "-stderr-budget")
            stdout, stderr = out_path.read_text(), err_path.read_text()
            private_write(stem.with_suffix(".requests.json"), fixture.requests)
            private_write(stem.with_suffix(".result.json"), {"exit_code": actual})
            require(self.password not in stdout + stderr, name + "-credential-disclosure")
            require(actual == code, name + "-exit")
            require(not fixture.unexpected, name + "-unexpected-request")
            frames = [json.loads(line) for line in stdout.splitlines()]
            require(all(isinstance(frame, dict) and frame.get("schema_version") == 1
                        and frame.get("api_version") == 2 for frame in frames), name + "-envelope")
            errors = [json.loads(line) for line in stderr.splitlines() if line.startswith("{")]
            return fixture, frames, errors, stdout

    def record(self, name):
        self.checks.append({"name": name, "status": "passed"})

    def execute(self):
        fixture, frames, _, stdout = self.run("watermark-reconnect", "reconnect",
            ["watch", "--samples", "5", "--interval-ms", "250"])
        require(len(frames) == 5 and fixture.observation_reads == 5, "reconnect-bounded-frames")
        data = [frame["data"] for frame in frames]
        require(all(frame["kind"] == "watch" for frame in frames), "reconnect-watch-kind")
        require(data[0]["data"]["cursor"] == "epoch-a:10", "reconnect-initial-watermark")
        require(data[1]["state"] == "old_revision_discarded" and data[1]["data"] is None,
                "reconnect-old-sequence-discarded")
        require("discard-this-marker" not in stdout, "reconnect-old-payload-not-retained")
        require(data[2]["stale"] and data[2]["data"] is None
                and data[2]["last_evidence"]["cursor"] == "epoch-a:10", "reconnect-stale-last-evidence")
        require(data[3]["state"] == "reconnected_resynchronized"
                and data[3]["data"]["cursor"] == "epoch-b:1", "reconnect-new-epoch")
        require(data[4]["state"] == "generation_changed"
                and data[4]["data"]["resources"][0]["body"]["run_generation"] == "2", "reconnect-new-generation")
        require([item["connection_epoch"] for item in data] == [1, 1, 1, 2, 3], "reconnect-local-epochs")
        require("last_evidence" not in data[3] and "last_evidence" not in data[4], "reconnect-old-data-not-merged")
        self.record("watch-sequence-reconnect-epoch-generation")

        _, frames, _, _ = self.run("resource-revisions", "resource-revisions",
            ["watch", "run-fixture", "--samples", "3", "--interval-ms", "250"])
        require(len(frames) == 3 and frames[1]["data"]["state"] == "old_revision_discarded"
                and frames[2]["data"]["state"] == "generation_changed", "resource-revision-guard")
        self.record("watch-resource-revision-and-generation")

        for name, status, code in [("forbidden", 3, "FORBIDDEN"), ("not-found", 4, "NOT_FOUND")]:
            fixture, frames, errors, _ = self.run("watch-" + name, name,
                ["watch", "--samples", "5", "--interval-ms", "250"], code=status)
            require(len(frames) == 1 and fixture.observation_reads == 2, name + "-stops-without-retry")
            require(any(error.get("kind") == "error" and error.get("data", {}).get("code") == code
                        for error in errors), name + "-structured-error")
            self.record("watch-" + name + "-stops")
            fixture, frames, errors, _ = self.run("identity-" + name, "resource-" + name,
                ["watch", "run-fixture", "--samples", "5", "--interval-ms", "250"], code=status)
            require(len(frames) == 1 and fixture.resource_reads == 2, name + "-identity-stops-without-retry")
            require(any(error.get("data", {}).get("code") == code for error in errors), name + "-identity-error")
            self.record("watch-resource-identity-" + name + "-stops")

        fixture, frames, _, _ = self.run("watch-interrupt", "interrupt",
            ["watch", "--samples", "100", "--interval-ms", "250"], interrupt=True)
        require(1 <= len(frames) <= 2 and fixture.observation_reads <= 2, "interrupt-local-only")
        require(all(item["path"] == "/api/v2/session" or item["method"] == "GET"
                    for item in fixture.requests), "interrupt-no-lifecycle-request")
        self.record("watch-sigint-stops-local-observation")

        fixture, frames, _, _ = self.run("log-flood", "log-flood",
            ["logs", "run-fixture", "--follow", "--samples", "2", "--interval-ms", "250"])
        require(len(frames) == 2 and fixture.log_reads == 2, "log-follow-bounded")
        require(all(frame["kind"] == "logs" and len(frame["data"]["page"]["text"]) == 65_536
                    for frame in frames), "log-page-capacity")
        require([frame["data"]["page"]["offset"] for frame in frames] == ["0", "65536"], "log-byte-offsets")
        require([frame["data"]["page"]["next_offset"] for frame in frames] == ["65536", "131072"], "log-next-byte-offsets")
        require(all(frame["data"]["generation"] == "1" for frame in frames), "log-pinned-generation")
        self.record("logs-fixed-worker-pages-and-bounded-flood")

        fixture, frames, errors, _ = self.run("log-source-change", "log-source-change",
            ["logs", "run-fixture"], code=5)
        require(not frames and fixture.log_reads == 1, "log-changed-source-page-discarded")
        require(any(error.get("data", {}).get("code") == "STALE_GENERATION" for error in errors), "log-change-error")
        self.record("logs-generation-change-discards-inflight-page")


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
    if args.output.exists():
        parser.error("--output must be a new private directory")
    if not args.binary.is_file():
        parser.error("--binary must be an existing server-built executable")
    os.umask(0o077)
    args.output.mkdir(parents=True, mode=0o700)
    matrix = Matrix(args)
    summary = {"schema_version": 1, "scope": "scripted loopback observation protocol only",
               "redaction": "allowlisted names/status/hashes only; raw process evidence private",
               "binary_sha256": digest(args.binary), "script_sha256": digest(Path(__file__).resolve()),
               "passed": False, "checks": [], "uncovered": ["real manager authorization implementation",
               "SSE streaming", "runtime/account facts and ledger", "lifecycle completion", "venue access"]}
    try:
        matrix.execute()
        summary["passed"] = True
    except Exception as error:
        private_write(args.output / "failure.txt", traceback.format_exc())
        summary["failure"] = str(error) if isinstance(error, CheckFailure) else "private-evidence-required"
    finally:
        summary["checks"] = matrix.checks
        private_write(args.output / "summary.json", summary)
        print(json.dumps(summary), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
