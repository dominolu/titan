"""Build-server research query regression: large history cannot lock out admission."""
import argparse
import concurrent.futures
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import sqlite3
import subprocess
import threading
import time
from urllib import request, error


def main():
    parser = argparse.ArgumentParser()
    for name in ("repo", "binary", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    assert "192.168.3.88" in subprocess.check_output(["hostname", "-I"]).decode()
    repo, binary, root = args.repo.resolve(), args.binary.resolve(), args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    root.chmod(0o700)

    def port():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    origin = f"http://127.0.0.1:{port()}"
    password = secrets.token_hex(24)
    hashed = subprocess.check_output([str(binary), "console", "password-hash"], input=(password + "\n").encode()).decode().strip()
    quote = lambda value: json.dumps(str(value))
    database, config = root / "manager.db", root / "console.toml"
    config.write_text("\n".join([
        "schema_version=1", "[manager]", f"listen={quote(origin.removeprefix('http://'))}",
        f"public_origin={quote(origin)}", "secure_cookie=false", f"database={quote(database)}",
        f"web_root={quote(repo / 'web/console/dist')}", "[manager.nodes.build]",
        f"endpoint={quote('http://127.0.0.1:' + str(port()))}", 'token_env="TITAN_RESEARCH_TEST_TOKEN"',
        'environments=["backtest"]', "[[manager.users]]", 'username="operator"',
        f"password_hash={quote(hashed)}", 'role="operator"', 'nodes=["build"]',
        "accounts=[]", 'environments=["backtest"]',
    ]) + "\n")
    config.chmod(0o600)
    client = request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    csrf = ""

    def call(path, data=None):
        req = request.Request(origin + path, data=None if data is None else json.dumps(data).encode(), headers={
            "Origin": origin, "Content-Type": "application/json", "X-CSRF-Token": csrf,
        })
        with client.open(req, timeout=30) as response:
            return response.status, json.load(response)

    with (root / "manager.log").open("wb") as log:
        process = subprocess.Popen([str(binary), "console", "manager", "--config", str(config)],
                                   env=dict(os.environ, TITAN_RESEARCH_TEST_TOKEN=secrets.token_hex(32)),
                                   stdout=log, stderr=log, start_new_session=True)
        try:
            for _ in range(100):
                try:
                    _, session = call("/api/v1/session", {"username": "operator", "password": password})
                    csrf = session["csrf_token"]
                    break
                except (error.URLError, ConnectionError):
                    time.sleep(0.1)
            else:
                raise RuntimeError("isolated manager did not start")

            def item(identity, kind, body):
                return {"id": identity, "kind": kind, "node_id": "build", "revision": "1", "frozen": False,
                        "digest": "synthetic-fixture", "body": {"environment": "backtest", **body}}

            # The longest accepted configured ID (119) becomes a valid 128-byte workspace ID.
            research_id = "research-" + "a" * 119
            template = item("build-template-boundary", "template", {
                "research_id": research_id, "research_name": "boundary", "template_id": "boundary",
                "source_digest": "b" * 64, "parameters": {"quantity": "9007199254740993"},
            })
            with sqlite3.connect(database) as db:
                db.execute("INSERT INTO resources VALUES (?,?,?,?)", (template["id"], "template", "build", json.dumps(template)))
            status, catalog = call("/api/v1/research")
            assert status == 200 and any(row["id"] == research_id for row in catalog)
            status, rows = call("/api/v1/resources?research_id=" + research_id + "&kind=template")
            assert status == 200 and rows == [template]

            count, padding = 12000, "x" * 32768
            with sqlite3.connect(database) as db:
                db.executemany("INSERT INTO resources VALUES (?,?,?,?)", (
                    (f"history-{index}", "artifact", "build", json.dumps(item(f"history-{index}", "artifact", {
                        "research_id": "research-archive", "research_name": "archive", "metadata": {"padding": padding},
                    }))) for index in range(count)))
            timings = {}
            for route in ("/api/v1/research", "/api/v1/resources?research_id=research-empty&kind=artifact&limit=20"):
                start = time.monotonic()
                status, values = call(route)
                assert status == 200
                timings[route] = time.monotonic() - start
                if "research-empty" in route:
                    assert values == []

            entered = threading.Event()

            def history():
                entered.set()
                for _ in range(3):
                    call("/api/v1/research")

            concurrent_times = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                scan = pool.submit(history)
                entered.wait(timeout=2)
                time.sleep(0.05)
                assert not scan.done(), "history fixture did not overlap admission probes"
                for index in range(6):
                    start = time.monotonic()
                    if index % 2:
                        status, saved = call("/api/v1/parameter-versions", {
                            "node_id": "build", "template_id": "boundary", "parameters": {"probe": index},
                            "expected_revision": None, "freeze_operation_id": None,
                        })
                        assert saved["body"]["research_id"] == research_id
                    else:
                        status, _ = call("/api/v1/resources/" + template["id"])
                    assert status in (200, 201)
                    concurrent_times.append(time.monotonic() - start)
                scan.result()
            assert max(concurrent_times) < 0.5, concurrent_times
            result = {"passed": True, "synthetic_history_records": count, "metadata_bytes_per_record": len(padding),
                      "valid_workspace_id_length": len(research_id), "history_query_seconds": timings,
                      "concurrent_read_and_admission_seconds": concurrent_times,
                      "maximum_concurrent_seconds": max(concurrent_times), "real_account_access": False}
            (root / "results.json").write_text(json.dumps(result, indent=2))
            print(json.dumps(result), flush=True)
        finally:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":
    main()
