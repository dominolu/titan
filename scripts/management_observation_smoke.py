#!/usr/bin/env python3
"""Build-server M2 protocol/browser faults using an E-produced synthetic account image."""
import argparse
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request, error, parse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--browser", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    image = json.loads(args.fixture.read_text())
    assert image["source"] == "test_account_service"
    state = {"stale": False, "condition": "one"}
    token, password = secrets.token_hex(32), secrets.token_hex(16)

    class Node(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try: self.wfile.write(data)
            except BrokenPipeError: pass
        def do_GET(self):
            url = parse.urlparse(self.path)
            if url.path == "/fault/stale":
                state["stale"] = True
                return self.reply(200, {})
            if url.path == "/fault/recover":
                state.update(stale=False, condition="two")
                return self.reply(200, {})
            if self.headers.get("Authorization") != "Bearer " + token:
                return self.reply(401, {"code": "UNAUTHENTICATED"})
            if self.server.slow:
                time.sleep(12)
                return self.reply(200, [])
            if url.path == "/v1/catalog":
                body = {"account": "gate", "instrument": "BTC_USDT", "worker_id": "fixture", "generation": "one", "environment": "live", "mode": "observe", "process_state": "RUNNING", "available": not state["stale"], "observation_stale": state["stale"], "observation": image, "allowed_actions": []}
                def resource(kind, name, contents):
                    return {"id": name, "kind": kind, "node_id": "observe", "revision": "", "frozen": False, "digest": "", "body": contents}
                return self.reply(200, [resource("run", "observe-run-fixture", body), resource("account", "observe-account-gate", body), resource("alert", "observe-alert", {"environment": "live", "account": "gate", "source": "E_fixture_diagnostics", "code": "TEST_OBSERVATION", "condition_identity": state["condition"], "active": True, "message": "synthetic source for browser verification"})])
            if url.path == "/v1/runs/fixture/history":
                kind = parse.parse_qs(url.query).get("kind", [""])[0]
                return self.reply(200, {"items": image["account_snapshot"]["image"].get("orders", []) if kind == "order" else [], "recording": {"history_complete": False}, "source": {"worker_id": "fixture", "generation": "one", "scope": "account", "synthetic": True}})
            if url.path == "/v1/runs/fixture/log":
                return self.reply(200, {"text":"synthetic worker log", "next_offset":"20", "more":False})
            return self.reply(404, {"code": "NOT_FOUND", "message": "not admitted", "request_id": "test"})

    nodes = []
    for slow in (False, True):
        node = ThreadingHTTPServer(("127.0.0.1", 0), Node)
        node.daemon_threads = True
        node.slow = slow
        threading.Thread(target=node.serve_forever, daemon=True).start()
        nodes.append(node)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]
    origin = f"http://127.0.0.1:{port}"
    env = {**os.environ, "TITAN_CONSOLE_NODE_TOKEN": token, "TITAN_BROWSER_PASSWORD": password}
    hashed = subprocess.check_output([str(args.binary), "console", "password-hash"], input=(password + "\n").encode(), env=env).decode().strip()
    q = lambda value: json.dumps(str(value))
    configuration = ["schema_version=1", "[manager]", f'listen="127.0.0.1:{port}"', f"public_origin={q(origin)}", "secure_cookie=false", f"database={q(args.output / 'manager.db')}", f"web_root={q(args.repo / 'web/console/dist')}"]
    for name, node in zip(("observe", "slow"), nodes):
        configuration += [f"[manager.nodes.{name}]", f'endpoint="http://127.0.0.1:{node.server_port}"', 'token_env="TITAN_CONSOLE_NODE_TOKEN"', 'environments=["live"]']
    for name, role, accounts in (("operator", "operator", ["gate"]), ("reader", "reader", ["gate"]), ("hidden", "reader", [])):
        configuration += ["[[manager.users]]", f"username={q(name)}", f"password_hash={q(hashed)}", f"role={q(role)}", 'nodes=["observe","slow"]', f"accounts={json.dumps(accounts)}", 'environments=["live"]']
    config_path = args.output / "console.toml"
    config_path.write_text("\n".join(configuration))
    config_path.chmod(0o600)
    log = (args.output / "manager.log").open("wb")
    process = subprocess.Popen([str(args.binary), "console", "manager", "--config", str(config_path)], env=env, stdout=log, stderr=log)
    opener = request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    def call(path, data=None, csrf=None, who=opener):
        headers = {"Origin": origin, "Content-Type": "application/json"}
        if csrf: headers["X-CSRF-Token"] = csrf
        with who.open(request.Request(origin + path, data=None if data is None else json.dumps(data).encode(), headers=headers), timeout=5) as response:
            return json.load(response)
    def wait(callback, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = callback()
                if value: return value
            except (error.URLError, TimeoutError, ConnectionError): pass
            time.sleep(0.1)
        raise AssertionError("observation verification timed out")
    checks = []
    try:
        session = wait(lambda: call("/api/v1/session", {"username": "operator", "password": password}))
        resources = wait(lambda: [r for r in call("/api/v1/snapshot")["resources"] if r["kind"] == "account"])
        assert resources[0]["body"]["observation"] == image
        checks += ["E-image-propagates-without-financial-transformation", "slow-node-does-not-block-other-node"]
        history = call("/api/v1/resources/observe-run-fixture/history?generation=one&kind=order&limit=20")
        assert len(history["items"]) == 2
        for path, data, expected in [
            ("/api/v1/resources/observe-run-fixture/history?generation=stale", None, 409),
            ("/api/v1/operations", {"idempotency_key": "readonly-node", "node_id": "observe", "action": {"kind": "pause", "run_id": "observe-run-fixture", "generation": "one"}}, 403),
        ]:
            try: call(path, data, session["csrf_token"]); raise AssertionError("unsafe request accepted")
            except error.HTTPError as failure: assert failure.code == expected
        checks += ["old-generation-rejected", "readonly-node-control-rejected"]
        hidden = request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        call("/api/v1/session", {"username": "hidden", "password": password}, who=hidden)
        assert not any(r["kind"] in ("run", "account", "alert") for r in call("/api/v1/snapshot", who=hidden)["resources"])
        for suffix in ("history", "log"):
            try: call("/api/v1/resources/observe-run-fixture/" + suffix, who=hidden); raise AssertionError("unauthorized account query accepted")
            except error.HTTPError as failure: assert failure.code == 403
        checks += ["unauthorized-account-is-hidden"]
        if args.browser:
            subprocess.run(["node", str(args.repo / "scripts/management_observation_browser.mjs"), origin, f"http://127.0.0.1:{nodes[0].server_port}", str(args.output)], env=env, check=True, timeout=100)
            checks += ["browser-account-history-diagnostics-ack-stale-reconnect-reader"]
        (args.output / "results.json").write_text(json.dumps({"passed": True, "source": "synthetic E output, no real account", "checks": checks}, indent=2))
        print(json.dumps({"passed": True, "checks": checks}), flush=True)
    finally:
        process.send_signal(signal.SIGINT)
        try: process.wait(timeout=10)
        except subprocess.TimeoutExpired: process.kill(); process.wait()
        log.close()
        for node in nodes: node.shutdown()


if __name__ == "__main__": main()
