#!/usr/bin/env python3
"""Controlled M2 acceptance. Run only on the explicitly authorized live host.

Credentials remain in its private directory; this program only makes GET venue requests.
The account backend is read through SQLite mode=ro, without initializing its service registry.
"""
import argparse
from decimal import Decimal
import hashlib
import hmac
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from urllib import request, error, parse

SYMBOL = "GATE_FUTURE_BTC_USDT"

def ready_run(resources):
    for resource in resources:
        if resource["kind"]!="run":continue
        observation=resource["body"].get("observation") or {}
        if (observation.get("account_snapshot") or {}).get("available"):return resource
    return None


def write_private(path, text):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(text)


def decimal_unit(value):
    value = Decimal(str(value)).normalize()
    assert value.is_finite() and value > 0
    _, digits, exponent = value.as_tuple()
    coefficient = int("".join(map(str, digits)))
    if exponent >= 0: coefficient *= 10 ** exponent
    scale = max(0, -exponent)
    assert coefficient < 2 ** 64 and scale <= 18
    return "{coefficient=" + str(coefficient) + ",scale=" + str(scale) + "}"


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--web-root", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--account-backend", type=Path, default=Path("/data/gate_arb"))
    args = parser.parse_args()
    assert all(path.is_absolute() for path in (args.root,args.binary,args.web_root,args.account_backend)), "absolute paths are required"
    assert sys.platform == "linux" and Path("/data/gate_arb").is_dir(), "live host is required"
    assert hashlib.file_digest(args.binary.open("rb"), "sha256").hexdigest() == args.expected_sha256
    args.root.mkdir(mode=0o700)
    secret_root = args.root / "secrets"
    secret_root.mkdir(mode=0o700)
    # Imports only configuration and Fernet support, never the account service application.
    os.chdir(args.account_backend)
    sys.path.insert(0, str(args.account_backend))
    from app.core.config import settings
    from app.services.account_credentials import AccountCredentialService
    from sqlalchemy.engine import make_url
    database = Path(make_url(settings.resolved_database_url).database).resolve()
    with sqlite3.connect("file:" + parse.quote(str(database)) + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT * FROM exchange_accounts WHERE id=?", (1,)).fetchone()
    assert row and row["name"] == "gate" and row["account_type"] == "crossex" and row["exchange"] == "GATE" and row["enabled"], "authorized account identity changed"
    credentials = AccountCredentialService().decrypt(row["credentials_encrypted"])
    key, secret = credentials["api_key"], credentials["api_secret"]
    assert key and secret
    write_private(secret_root / "gate.toml", "api_key=" + json.dumps(key) + "\nsecret=" + json.dumps(secret) + "\n")

    def venue(path, query=(), authenticated=True):
        assert path in ("/crossex/rule/symbols", "/crossex/accounts", "/crossex/positions", "/crossex/open_orders")
        encoded = parse.urlencode(query)
        headers = {"Accept": "application/json"}
        if authenticated:
            timestamp = str(int(time.time()))
            payload = "\n".join(("GET", "/api/v4" + path, encoded, hashlib.sha512(b"").hexdigest(), timestamp))
            headers.update(KEY=key, Timestamp=timestamp, SIGN=hmac.new(secret.encode(), payload.encode(), hashlib.sha512).hexdigest())
        url = "https://api.gateio.ws/api/v4" + path + ("?" + encoded if encoded else "")
        with request.urlopen(request.Request(url, headers=headers, method="GET"), timeout=12) as response:
            data = response.read(8 * 1024 * 1024 + 1)
            assert len(data) <= 8 * 1024 * 1024, "venue reply limit exceeded"
            return json.loads(data)

    rules = venue("/crossex/rule/symbols", authenticated=False)
    rule = next(item for item in rules if item["symbol"] == SYMBOL)
    assert rule["business_type"] == "FUTURE" and rule["state"] == "live"
    account = venue("/crossex/accounts")
    assert account["account_mode"] == "CROSS_EXCHANGE" and account["position_mode"] == "SINGLE", "account mode differs from the authorized baseline"
    before_positions = [p for p in venue("/crossex/positions") if p.get("symbol") == SYMBOL]
    before_orders = [o for o in venue("/crossex/open_orders", (("symbol", SYMBOL),)) if o.get("symbol") == SYMBOL]
    q = lambda value: json.dumps(str(value))
    runtime = ["schema_version=1", "[account_service]", f"secret_root={q(secret_root)}", "startup_timeout_ms=30000", "[[accounts]]", 'account_key="gate"', "account_id=1", 'connector_type="gate-crossex-account"', 'credential_ref="secret://file/gate.toml"', 'connector_config=""', 'ownership="ObserveAll"', 'shutdown_order_policy="LeaveOpen"', "enabled=true", "definition_version=1", "[[accounts.instruments]]", f"native_symbol={q(SYMBOL)}", "asset_id=1", "price_tick=" + decimal_unit(rule["tick_size"]), "quantity_lot=" + decimal_unit(rule["lot_size"]), "contract_multiplier={coefficient=1,scale=0}", "[[accounts.currencies]]", 'native_currency="USDT"', "currency_id=1", "amount_unit={coefficient=1,scale=8}"]
    runtime_path = args.root / "runtime.toml"
    write_private(runtime_path, "\n".join(runtime) + "\n")
    manager_port, node_port = port(), port()
    origin = f"http://127.0.0.1:{manager_port}"
    token, password = secrets.token_hex(32), secrets.token_hex(24)
    env = {**os.environ, "TITAN_CONSOLE_LIVE_TOKEN": token}
    password_hash = subprocess.check_output([str(args.binary), "console", "password-hash"], input=(password + "\n").encode(), env=env).decode().strip()
    config = ["schema_version=1", "[manager]", f'listen="127.0.0.1:{manager_port}"', f"public_origin={q(origin)}", "secure_cookie=false", f"database={q(args.root / 'manager.sqlite3')}", f"web_root={q(args.web_root)}", "[manager.nodes.live]", f'endpoint="http://127.0.0.1:{node_port}"', 'token_env="TITAN_CONSOLE_LIVE_TOKEN"', 'environments=["live"]', "live_control=false"]
    for name, role in (("operator", "operator"), ("reader", "reader")):
        config += ["[[manager.users]]", f"username={q(name)}", f"password_hash={q(password_hash)}", f"role={q(role)}", 'nodes=["live"]', 'accounts=["gate"]', 'environments=["live"]']
    config += ["[node]", 'id="live"', f'listen="127.0.0.1:{node_port}"', 'token_env="TITAN_CONSOLE_LIVE_TOKEN"', f"work_root={q(args.root / 'node')}", f"titan_program={q(args.binary)}", 'environments=["live"]', 'observe_bindings=["gate-btc"]', "live_control=false", "[node.bindings.gate-btc]", 'account="gate"', f"instrument={q(SYMBOL)}", f"runtime_config={q(runtime_path)}", 'strategy_key="grid"', 'max_exposure_quote="100"', "max_price_deviation_bps=50", "max_duration_seconds=90"]
    configuration = args.root / "console.toml"
    write_private(configuration, "\n".join(config) + "\n")
    write_private(args.root / "credential-source.json", json.dumps({"account_id": 1, "name": "gate", "exchange": "GATE", "account_type": "crossex", "credential_fingerprint": row["credential_fingerprint"]}, indent=2))
    opener = request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    def call(path, data=None, csrf=None):
        headers = {"Origin": origin, "Content-Type": "application/json"}
        if csrf: headers["X-CSRF-Token"] = csrf
        with opener.open(request.Request(origin + path, data=None if data is None else json.dumps(data).encode(), headers=headers), timeout=8) as response:
            return json.load(response)
    def wait(callback, timeout=40):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = callback()
                if value: return value
            except (error.URLError, TimeoutError, ConnectionError): pass
            time.sleep(0.2)
        raise RuntimeError("controlled observation timed out; inspect private host logs")
    processes, logs = [], []
    def start(kind):
        log = (args.root / (kind + ".log")).open("ab")
        logs.append(log)
        process = subprocess.Popen([str(args.binary), "console", kind, "--config", str(configuration)], env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        processes.append(process)
        return process
    def stop(process):
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try: process.wait(timeout=8)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
    run = None
    started = time.monotonic()
    evidence = {"host": "43.130.234.54", "binary_sha256": args.expected_sha256, "account_id": 1, "account": "gate", "instrument": SYMBOL, "max_exposure_quote": "100", "max_price_deviation_bps": 50, "max_observation_seconds": 90, "mode": "observe", "venue_methods": ["GET"], "preexisting_target_orders": len(before_orders), "preexisting_target_positions": [{k: p.get(k) for k in ("symbol", "position_side", "position_qty")} for p in before_positions]}
    try:
        node, manager = start("node"), start("manager")
        session = wait(lambda: call("/api/v1/session", {"username": "operator", "password": password}))
        def current():
            resources = call("/api/v1/snapshot")["resources"]
            return ready_run(resources)
        run = wait(current)
        observation = run["body"]["observation"]
        assert observation["readonly"] and observation["strategies"] == [] and observation["account_snapshot"]["image"]["account_id"] == "1"
        write_private(args.root / "snapshot-private.json", json.dumps(run, indent=2))
        identity = (run["id"], run["body"]["generation"], observation["pid"], observation["start_time"])
        try:
            call("/api/v1/operations", {"idempotency_key": "live-readonly-rejection", "node_id": "live", "action": {"kind": "pause", "run_id": run["id"], "generation": run["body"]["generation"]}}, session["csrf_token"])
            raise AssertionError("live write capability was unexpectedly enabled")
        except error.HTTPError as failure: assert failure.code == 403
        history = call(f"/api/v1/resources/{run['id']}/history?kind=balance&generation={run['body']['generation']}")
        assert history["items"], "account recorder did not retain bootstrap balances"
        assert all(item["account_id"] == "1" for item in history["items"])
        stop(manager); stop(node)
        node, manager = start("node"), start("manager")
        recovered = wait(current, timeout=15)
        actual = recovered["body"]["observation"]
        assert (recovered["id"], recovered["body"]["generation"], actual["pid"], actual["start_time"]) == identity, "manager/node restart created or adopted a different worker"
        wait(lambda: call("/api/v1/snapshot") and next((r for r in call("/api/v1/snapshot")["resources"] if r["id"] == run["id"] and r["body"]["process_state"] in ("STOPPED", "FAILED", "UNKNOWN")), None), timeout=max(1, 112 - (time.monotonic() - started)))
        with sqlite3.connect(args.root / "node/runs.sqlite3") as db:
            rows = db.execute("SELECT state FROM runs WHERE strategy_id='management-observer'").fetchall()
        assert rows == [("STOPPED",)], "observer did not finish cleanly or was duplicated"
        post_positions = [p for p in venue("/crossex/positions") if p.get("symbol") == SYMBOL]
        post_orders = [o for o in venue("/crossex/open_orders", (("symbol", SYMBOL),)) if o.get("symbol") == SYMBOL]
        evidence.update(passed=True, readonly_write_rejected=True, restart_preserved_worker=True, worker_count=1, final_process_state="STOPPED", target_orders_after=len(post_orders), target_positions_after=[{k: p.get(k) for k in ("symbol", "position_side", "position_qty")} for p in post_positions], elapsed_seconds=round(time.monotonic() - started, 3), recording_db_retained=True)
        write_private(args.root / "results.json", json.dumps(evidence, indent=2))
        print(json.dumps(evidence), flush=True)
    finally:
        for process in reversed(processes): stop(process)
        # This signal is scoped to the exact worker identity, never to a strategy or another process.
        if run:
            snapshot = run["body"]["observation"]
            pid = int(snapshot["pid"])
            stat = Path(f"/proc/{pid}/stat")
            if stat.exists():
                fields = stat.read_text().rsplit(")", 1)[1].split()
                if fields[19] == snapshot["start_time"]:
                    os.kill(pid, signal.SIGINT)
        for log in logs: log.close()


if __name__ == "__main__": main()
