#!/usr/bin/env python3
"""Build-host-only A/N/E acceptance of an immutable signed candidate, using loopback Gate."""
import argparse
import hashlib
import http.cookiejar
import json
import os
import secrets
import shutil
import signal
import socket
import sqlite3
import subprocess
import threading
import time
from decimal import Decimal, ROUND_HALF_UP
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib import error, parse, request

from live_fill_acceptance import checkpoint_has_position, validate_parameters
from management_control_smoke import Handler as WebSocketHandler, Venue as WebSocketVenue, port, toml, unit

SYMBOL = "GATE_FUTURE_BTC_USDT"
EXPECTED_BINARY = "9649e59d4cb97d1b8e4be25b2081960d2b728573bf5fd25507fff7bc827d6929"
EXPECTED_STRATEGY = "dcc71c4a2594641a1d8368b5f248ffcede8b622230eee491f23e453dba616d8a"


def digest(path):
    return hashlib.file_digest(path.open("rb"), "sha256").hexdigest()


def read_db(path):
    return sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True)


class FillVenue(WebSocketVenue):
    def __init__(self, checkpoint):
        ThreadingHTTPServer.__init__(self, ("127.0.0.1", 0), FillHandler)
        self.lock = threading.RLock()
        self.orders, self.private, self.public = {}, [], []
        self.calls, self.failed, self.ws_frames, self.intent_proofs = [], [], [], []
        self.alive, self.recovery_database = True, checkpoint
        self.net, self.entry, self.wallet = Decimal(0), Decimal(0), Decimal(1000)
        self.trades, self.delivery, self.pending = [], [], []
        self.stamp = int(time.time() * 1000)

    def now(self):
        self.stamp = max(self.stamp + 1, int(time.time() * 1000))
        return str(self.stamp)

    def position(self):
        return {"symbol": SYMBOL, "position_side": "NONE", "position_qty": str(self.net),
                "entry_price": str(self.entry), "mark_price": "80800.0", "liq_price": "0",
                "leverage": "1", "margin_mode": "CROSS", "upnl": "0", "closed_pnl": "0",
                "position_value": str(abs(self.net) * Decimal("80800")), "update_time": str(self.stamp)}

    def account(self):
        return {"account_mode": "CROSS_EXCHANGE", "position_mode": "SINGLE",
                "margin_balance": str(self.wallet), "available_margin": str(self.wallet),
                "update_time": str(self.stamp), "assets": [{"coin": "USDT", "exchange_type": "GATE",
                "balance": str(self.wallet), "available_balance": str(self.wallet), "upnl": "0", "equity": str(self.wallet)}]}

    def prices(self):
        while self.alive:
            with self.lock:
                for stream in self.public[:]:
                    try:
                        stream.send_ws({"event": "update", "channel": "ticker", "result": {
                            "s": SYMBOL, "ts": int(time.time() * 1000), "bp": "80800.0", "bs": "1",
                            "ap": "80800.1", "as": "1"}})
                    except (OSError, ValueError):
                        self.public.remove(stream)
            time.sleep(.1)

    def send_trade(self, trade, count=1):
        for _ in range(count):
            self.delivery.append({"channel": "usertrades", "transaction_id": trade["transaction_id"],
                                  "process_phase": "restored" if self.replaying else "initial"})
            self.broadcast("usertrades", trade)

    replaying = False

    def release_opening(self):
        with self.lock:
            assert len(self.pending) == 1
            trade = self.pending.pop()
            self.send_trade(trade, 3)

    def durable_intent(self, client):
        order_id = int.from_bytes(bytes.fromhex(client)[8:], "little")
        with read_db(self.recovery_database) as db:
            for record, sha, payload in db.execute("SELECT id,sha256,payload FROM recovery_points WHERE strategy_no=77 ORDER BY rowid DESC"):
                point = json.loads(payload)
                if any(int(o["order_id"]) == order_id for o in point["orders_list"]):
                    assert hashlib.sha256(payload).hexdigest() == sha
                    proof = {"record_id": record, "payload_sha256": sha, "order_id": str(order_id),
                             "generation": point["generation"], "client_order_id": client}
                    self.intent_proofs.append(proof)
                    return
        raise AssertionError("simulator received POST before committed SQLite intent")

    def fill(self, body):
        with self.lock:
            self.durable_intent(body["text"])
            assert body["symbol"] == SYMBOL and body["type"] == "LIMIT" and body["time_in_force"] == "IOC"
            qty = Decimal(body["qty"])
            assert qty == Decimal("0.0001") and len(self.orders) < 2
            opening = body["side"] == "BUY"
            assert opening == (len(self.orders) == 0)
            assert body["reduce_only"] == ("false" if opening else "true")
            assert self.net == (Decimal(0) if opening else qty)
            price = Decimal("80800.1" if opening else "80800.0")
            assert Decimal(body["price"]) >= price if opening else Decimal(body["price"]) <= price
            assert abs(Decimal(body["price"]) - Decimal("80800.05")) / Decimal("80800.05") <= Decimal(".005")
            assert qty * max(price, Decimal(body["price"])) <= 100
            now, order_id = self.now(), str(2072784922592768 + len(self.orders))
            order = {**body, "order_id": order_id, "state": "NEW", "executed_qty": "0",
                     "executed_avg_price": "0", "create_time": now, "update_time": now, "position_side": "NONE"}
            self.orders[order_id] = order
            self.broadcast("order", dict(order))
            fee = (qty * price * Decimal(".0005")).quantize(Decimal(".00000001"), rounding=ROUND_HALF_UP)
            pnl = Decimal(0) if opening else qty * (price - self.entry)
            self.wallet += pnl - fee
            self.net = qty if opening else Decimal(0)
            self.entry = price if opening else Decimal(0)
            self.now()
            self.delivery.append({"channel": "position", "position_qty": str(self.net), "opening": opening})
            self.broadcast("position", self.position())
            order.update(state="FILLED", executed_qty=str(qty), executed_avg_price=str(price),
                         last_executed_qty=str(qty), last_executed_price=str(price), update_time=self.now(),
                         fee=str(fee), fee_coin="USDT")
            self.broadcast("order", dict(order))
            trade = {"symbol": SYMBOL, "transaction_id": str(2072784922594048 + len(self.trades)),
                     "order_id": order_id, "text": body["text"], "side": body["side"], "qty": str(qty),
                     "price": str(price), "fee": str(fee), "fee_coin": "USDT", "fee_rate": "0.0005",
                     "rpnl": str(pnl), "match_role": "TAKER", "position_mode": "SINGLE",
                     "position_side": "NONE", "create_time": self.now()}
            self.trades.append(trade)
            if opening:
                self.pending.append(trade)  # The driver first proves E consumed the position.
            else:
                self.send_trade(trade, 3)
            return dict(order)


class FillHandler(WebSocketHandler):
    def do_GET(self):
        if self.headers.get("Upgrade", "").lower() == "websocket":
            try:
                self.websocket()
            except (OSError, ValueError):
                pass
            return
        url = parse.urlparse(self.path)
        path = url.path.removeprefix("/api/v4/crossex/")
        self.server.calls.append(("GET", path))
        with self.server.lock:
            if path == "rule/symbols":
                return self.reply([{"symbol": SYMBOL, "business_type": "FUTURE", "state": "live",
                    "tick_size": "0.1", "lot_size": "0.0001", "min_size": "0.0001", "min_notional": "0",
                    "max_limit_size": "1", "max_market_size": "1", "contract_size": "1"}])
            if path == "accounts":
                return self.reply(self.server.account())
            if path == "positions":
                return self.reply([self.server.position()] if self.server.net else [])
            if path in ("open_orders", "account_book"):
                return self.reply([])
            if path == "history_trades":
                return self.reply(self.server.trades)
            if path.startswith("orders/"):
                key = parse.unquote(path.split("/")[-1])
                value = self.server.orders.get(key) or next((v for v in self.server.orders.values() if v["text"] == key), None)
                return self.reply(value or {"label": "ORDER_NOT_FOUND"}, 200 if value else 404)
        self.server.failed.append(("GET", self.path))
        self.reply({"label": "UNSUPPORTED"}, 400)

    def do_POST(self):
        path = parse.urlparse(self.path).path.removeprefix("/api/v4/crossex/")
        self.server.calls.append(("POST", path))
        try:
            assert path == "orders"
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            self.reply(self.server.fill(body))
        except Exception as exc:
            self.server.failed.append(("POST", path, repr(exc)))
            self.reply({"label": "SIMULATION_ASSERTION", "message": str(exc)}, 500)

    def do_DELETE(self):
        self.server.failed.append(("DELETE", self.path))
        self.reply({"label": "NO_OPEN_ORDERS"}, 400)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo", type=Path, required=True)
    p.add_argument("--release", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--expected-binary-sha256", default=EXPECTED_BINARY)
    p.add_argument("--expected-strategy-sha256", default=EXPECTED_STRATEGY)
    p.add_argument("--candidate-label", default="candidate04")
    p.add_argument("--scenario", choices=("standard", "partial", "bootstrap-replay", "market-reconnect", "private-disconnect", "abnormal-stop", "private-recovery", "missing-fill-recovery", "missing-history-incomplete", "missing-history-conflict", "private-history-fee-conflict"), default="standard")
    p.add_argument("--strategy-source", type=Path)
    args = p.parse_args()
    assert "192.168.3.88" in subprocess.check_output(["hostname", "-I"]).decode(), "build host only"
    repo, release, root = args.repo.resolve(), args.release.resolve(), args.output.resolve()
    assert not root.exists()
    root.mkdir(parents=True, mode=0o700)
    binary = release / "bin/titan"
    package = json.loads((release / "release-manifest.json").read_text())
    assert digest(binary) == args.expected_binary_sha256 == package["binary_sha256"]
    assert all(digest(release / n) == v["sha256"] for n, v in package["files"].items())
    source = root / "source"
    assert args.strategy_source is None or args.scenario == "partial", "only the partial-fill fixture uses a separately signed parameter variant"
    shutil.copytree(args.strategy_source or release / "strategy_version-source", source)
    manifest = json.loads((source / "manifest.json").read_text())
    version = manifest["strategy_version"]
    artifact = source / "operations" / version["body"]["operation_id"] / "strategy.titan"
    assert digest(artifact) == args.expected_strategy_sha256
    if args.strategy_source:
        assert manifest["parameters"]["order_qty_lots"] == manifest["parameters"]["max_position_lots"] == 2
        normalized = {**manifest["parameters"], "order_qty_lots": 1, "max_position_lots": 1}
        validate_parameters(normalized)
        assert normalized == package["parameters"], "partial variant must change only the two quantity parameters"
    else:
        assert args.expected_strategy_sha256 == package["strategy_file_sha256"]
        validate_parameters(manifest["parameters"])
    validation = subprocess.run([str(binary), "strategy", "validate", str(artifact), "--require-signature",
        "--trusted-key", "acceptance=" + manifest["public_key"], "--json"], capture_output=True, text=True, check=True)
    (root / "native-validation.json").write_text(validation.stdout)
    checkpoint = root / "node/checkpoints/simulation-candidate04-gate-1/77/runtime.db"
    safe_recovery = args.scenario in ("private-recovery", "missing-fill-recovery", "missing-history-incomplete", "missing-history-conflict", "private-history-fee-conflict")
    if safe_recovery:
        from management_safe_recovery_smoke import SafeRecoveryVenue
        venue = SafeRecoveryVenue(checkpoint, args.scenario)
    elif args.scenario == "standard":
        venue = FillVenue(checkpoint)
    else:
        from management_fill_fault_smoke import FaultVenue
        venue = FaultVenue(checkpoint, args.scenario)
    threading.Thread(target=venue.serve_forever, daemon=True).start()
    threading.Thread(target=venue.prices, daemon=True).start()
    vp, mp, np, bp = venue.server_port, port(), port(), port()
    origin = f"http://127.0.0.1:{mp}"
    cfg_values = {"rest_url": f"http://127.0.0.1:{vp}/api/v4", "public_ws_url": f"ws://127.0.0.1:{vp}/public",
                  "private_ws_url": f"ws://127.0.0.1:{vp}/private", "market_quantity_multipliers": {SYMBOL: 1.0}}
    assert all(parse.urlparse(v).hostname == "127.0.0.1" for k, v in cfg_values.items() if k.endswith("url"))
    cfg = toml(cfg_values)
    secret = root / "secrets"
    secret.mkdir(mode=0o700)
    (secret / "gate.toml").write_text('api_key="synthetic-only"\nsecret="synthetic-only"\n')
    (secret / "gate.toml").chmod(0o600)
    duration = lambda secs, nanos=0: {"secs": secs, "nanos": nanos}
    instrument = {"native_symbol": SYMBOL, "asset_id": 1, "price_tick": unit(1, 1), "quantity_lot": unit(1, 4)}
    runtime = {"schema_version": 1, "managed_by_console": True,
        "account_service": {"secret_root": str(secret), "startup_timeout_ms": 30000, "recording_environment": "simulation"},
        "strategy_service": {"trusted_ed25519_keys": {"acceptance": manifest["public_key"]}},
        "market_sources": [{"source_key": "gate-market", "connector_type": "gate-crossex", "connector_config": cfg,
                            "instruments": [instrument], "enabled": True, "definition_version": 1}],
        "accounts": [{"account_key": "gate", "account_id": 1, "connector_type": "gate-crossex-account",
            "credential_ref": "secret://file/gate.toml", "connector_config": cfg, "ownership": "ObserveAll",
            "shutdown_order_policy": "LeaveOpen", "instruments": [{**instrument, "contract_multiplier": unit(1), "linear_quote_currency": 1}],
            "currencies": [{"native_currency": "USDT", "currency_id": 1, "amount_unit": unit(1, 8)}],
            "enabled": True, "definition_version": 1}],
        "strategies": [{"strategy_key": "grid", "runtime_strategy_no": 77,
            "strategy_file": {"loader_type": "native-v13", "uri": "file://unused.titan", "expected_digest": [1] * 32},
            "markets": [{"local_market_no": 0, "local_asset_no": 0, "source_key": "gate-market", "asset_id": 1, "data_mode": "tick"}],
            "accounts": [{"local_account_no": 0, "account_key": "gate", "tradable_assets": [{"local_asset_no": 0, "asset_id": 1}]}],
            "risk_scope": "simulation-100-usdt", "runtime": {"async_lane_capacity": 256, "critical_reserve": 32,
                "reliable_pending_capacity": 32, "worker_policy": "park", "timer_capacity": 16, "max_commands_per_callback": 64,
                "checkpoint_interval": duration(60), "timer_interval": duration(0, 100000000),
                "max_handler_duration": duration(1), "startup_timeout": duration(30), "stop_timeout": duration(10)},
            "recovery": "fresh", "enabled": True, "run_config_revision": 1}]}
    (root / "runtime.toml").write_text(toml(runtime))
    token, password = secrets.token_hex(32), secrets.token_hex(24)
    env = {**os.environ, "TITAN_CONSOLE_NODE_TOKEN": token, "RUST_LOG": "warn"}
    hashed = subprocess.check_output([str(binary), "console", "password-hash"], input=password.encode(), env=env).decode().strip()
    remote = lambda pt, live: {"endpoint": f"http://127.0.0.1:{pt}", "token_env": "TITAN_CONSOLE_NODE_TOKEN",
                              "environments": ["live" if live else "backtest"], "live_control": live}
    build = {"id": "build", "listen": f"127.0.0.1:{bp}", "token_env": "TITAN_CONSOLE_NODE_TOKEN",
             "work_root": str(source), "titan_program": str(binary), "environments": ["backtest"]}
    node = {"id": "live", "listen": f"127.0.0.1:{np}", "token_env": "TITAN_CONSOLE_NODE_TOKEN",
        "work_root": str(root / "node"), "titan_program": str(binary), "environments": ["live"], "live_control": True,
        "bindings": {"gate-btc": {"account": "gate", "account_identity": "simulation-candidate04-gate-1", "instrument": SYMBOL,
            "runtime_config": str(root / "runtime.toml"), "strategy_key": "grid", "max_exposure_quote": "100",
            "max_price_deviation_bps": 50, "max_duration_seconds": 120}}, "strategy_sources": {"build": remote(bp, False)}}
    manager = {"listen": f"127.0.0.1:{mp}", "public_origin": origin, "database": str(root / "manager.sqlite3"),
        "web_root": str(release / "web"), "secure_cookie": False, "nodes": {"build": remote(bp, False), "live": remote(np, True)},
        "users": [{"username": "operator", "password_hash": hashed, "role": "operator", "nodes": ["build", "live"],
                   "accounts": ["gate"], "environments": ["backtest", "live"]}]}
    for name, value in (("build", {"node": build}), ("live", {"node": node}), ("manager", {"manager": manager})):
        (root / (name + ".toml")).write_text(toml({"schema_version": 1, **value}))
    processes, checks, operations = [], [], []
    csrf, active, run = None, None, None
    opener = request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def start(name, kind):
        with (root / (name + ".log")).open("ab") as log:
            proc = subprocess.Popen([str(binary), "console", kind, "--config", str(root / (name + ".toml"))],
                env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        processes.append(proc)
        return proc

    def stop(proc):
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    def call(path, data=None):
        headers = {"Origin": origin, "Content-Type": "application/json"}
        if csrf:
            headers["X-CSRF-Token"] = csrf
        with opener.open(request.Request(origin + path, data=None if data is None else json.dumps(data).encode(), headers=headers), timeout=8) as response:
            return json.load(response)

    def wait(fn, timeout=15):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            try:
                value = fn()
                if value:
                    return value
            except (error.URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(.1)
        raise AssertionError("finite loopback acceptance timed out; inspect preserved evidence")

    def resources(kind):
        return [v for v in call("/api/v2/snapshot")["resources"] if v["kind"] == kind]

    def current():
        return next((v for v in resources("run") if v["id"] == active["id"]), None)

    def set_active(value):
        nonlocal active
        active = value

    def submit(action):
        op = call("/api/v2/operations", {"idempotency_key": secrets.token_hex(16), "node_id": "live", "action": action})
        outcome = wait(lambda: v if (v := call("/api/v2/operations/" + op["id"]))["state"] in ("succeeded", "failed", "cancelled") else None, 35)
        operations.append({"id": outcome["id"], "kind": action["kind"], "state": outcome["state"]})
        assert outcome["state"] == "succeeded", {"error": outcome.get("error"), "result": outcome.get("result")}
        return outcome

    def resource(op, kind):
        return next(v for v in op["result"]["resources"] if v["kind"] == kind)

    def control(kind):
        return submit({"kind": kind, "run_id": active["id"], "expected_run_generation": active["body"]["run_generation"]})

    def passed(name):
        checks.append(name)
        (root / "checks-progress.json").write_text(json.dumps(checks, indent=2))
        print(name + ": passed", flush=True)

    def trades():
        path = root / "node/account-facts.sqlite3"
        if not path.exists():
            return {}
        with read_db(path) as db:
            return {k: {"digest": bytes(d).hex(), "details": json.loads(b)["details"], "source_version": json.loads(b)["version"]} for k, d, b in db.execute("SELECT trade_key,digest,body FROM trades")}

    def business():
        item = current()
        if not item:
            return None
        image = call(f"/api/v2/strategies/{item['body']['strategy_id']}/runs/{item['id']}/snapshot")
        observation = image["business_observation"]
        if observation.get("available"):
            assert int(observation["data"]["last_error"]) == 0, observation["data"]
            account = item["body"].get("observation", {}).get("account_snapshot", {}).get("image", {})
            if int(observation["data"]["expected_inventory_lots"]) == 1 and account.get("risk", {}).get("complete"):
                return image

    def point():
        with read_db(checkpoint) as db:
            row = db.execute("SELECT p.id,p.sha256,p.payload FROM recovery_heads h JOIN recovery_points p ON p.id=h.record_id WHERE h.strategy_no=77").fetchone()
        if row:
            record, sha, payload = row
            assert hashlib.sha256(payload).hexdigest() == sha
            return record, sha, payload, json.loads(payload)

    def history(item):
        path = f"/api/v2/strategies/{item['body']['strategy_id']}/runs/{item['id']}/facts?"
        return call(path + parse.urlencode({"kind": "fill", "run_generation": item["body"]["run_generation"], "limit": 200}))

    passed("frozen-" + args.candidate_label + "-binary-web-signature-and-native-bytes-verified")
    successful = False
    try:
        start("build", "node")
        nodep, managerp = start("live", "node"), start("manager", "manager")
        session = wait(lambda: call("/api/v2/session", {"username": "operator", "password": password}))
        csrf = session["csrf_token"]
        wait(lambda: resources("strategy_version") and resources("binding"))
        deployment = resource(submit({"kind": "deploy", "strategy_version_id": version["id"], "binding_id": "gate-btc"}), "deployment")
        deployed = root / "node/deployments" / deployment["body"]["operation_id"] / "strategy.titan"
        assert digest(deployed) == args.expected_strategy_sha256
        passed("A-to-N-signed-" + args.candidate_label + "-deployed-without-recompile")
        if args.scenario != "standard":
            if safe_recovery:
                from management_safe_recovery_smoke import run_case
            else:
                from management_fill_fault_smoke import run_case
            result = run_case({"root": root, "venue": venue, "args": args, "checkpoint": checkpoint,
                "call": call, "wait": wait, "resources": resources, "current": current, "set_active": set_active,
                "submit": submit, "resource": resource, "control": control, "passed": passed, "trades": trades,
                "point": point, "history": history, "start": start, "stop": stop, "nodep": nodep, "managerp": managerp,
                "deployment": deployment, "operations": operations, "checks": checks, "connector_urls": cfg_values})
            (root / "results.json").write_text(json.dumps(result, indent=2))
            successful = result["passed"]
            return
        began = time.monotonic()
        run = resource(submit({"kind": "start", "deployment_id": deployment["id"]}), "run")
        active = run

        def position_before_fill():
            if not checkpoint.exists():
                return None
            with read_db(root / "node/account-facts.sqlite3") as db:
                positions = [json.loads(b) for b, in db.execute("SELECT body FROM facts WHERE kind='position'")]
            return next((v for v in positions if int(v["details"]["quantity_lots"]) == 1), None)

        first_position = wait(position_before_fill, 5)
        assert not trades() and venue.net == Decimal(".0001") and len(venue.pending) == 1
        passed("E-consumes-position-before-native-execution-and-fee")
        venue.release_opening()
        first = wait(lambda: trades() or None, 5)
        assert len(first) == 1
        opening = next(iter(first.values()))
        assert int(first_position["version"]) < int(opening["source_version"])
        (root / "ordering-proof.json").write_text(json.dumps({"passed": True, "position_version": first_position["version"],
            "native_fill_version": opening["source_version"], "position_lots": 1, "native_execution_rows_before_release": 0,
            "same_session_native_delivery_count": 3}, indent=2))
        assert int(opening["details"]["last_fill_quantity_lots"]) == 1
        assert int(opening["details"]["fee_amount_units"]) == 404001
        opening_digest = {k: v["digest"] for k, v in first.items()}
        wait(business, 5)
        control("pause")
        passed("native-fill-fee-and-same-session-replays-count-once")
        # Let the original 60 s interval run beyond the old 10 s strategy failure deadline.
        selected = wait(lambda: v if (v := point()) and checkpoint_has_position(v[-1]) else None, 70)
        record, sha, payload, saved = selected
        before = wait(business, 5)
        assert int(before["business_observation"]["data"]["last_error"]) == 0
        assert {k: v["digest"] for k, v in trades().items()} == opening_digest
        assert (int(run["body"]["expires_at_ns"]) - time.time_ns()) / 1e9 >= 40
        observed = wait(lambda: r if (r := current())["body"].get("recovery_reference", {}).get("record_id") == record else None, 5)
        ref = observed["body"]["recovery_reference"]
        assert ref["payload_sha256"] == sha
        (root / "checkpoint-proof.json").write_text(json.dumps({"passed": True, "reference": ref,
            "elapsed_seconds": time.monotonic() - began, "periodic_seconds": 60, "confirmed_position_lots": 1,
            "pending_execution": saved["recovery"]["pending_execution"], "business": before["business_observation"]["data"]}, indent=2))
        passed("original-60s-SQLite-point-retains-consumed-fill-and-healthy-business-state")
        obs = observed["body"]["observation"]
        old_pid, old_stamp = int(obs["pid"]), int(obs["start_time"])
        assert int(Path(f"/proc/{old_pid}/stat").read_text().rsplit(")", 1)[1].split()[19]) == old_stamp
        os.kill(old_pid, signal.SIGKILL)
        wait(lambda: (r if r["body"]["process_state"] == "UNKNOWN" else None) if (r := current()) else None)
        with read_db(root / "manager.sqlite3") as db:
            assert db.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0] == 2
        # Restart both A and N as well: all ownership and recovery must come from durable records.
        stop(managerp)
        stop(nodep)
        nodep, managerp = start("live", "node"), start("manager", "manager")
        wait(lambda: call("/api/v2/snapshot"))
        passed("owned-E-crash-and-A-N-restart-preserve-reference-and-reservation")
        restored = submit({"kind": "restore", "run_id": run["id"], "expected_run_generation": run["body"]["run_generation"], "recovery_point_sha256": sha})
        active = resource(restored, "run")
        assert int(active["body"]["run_generation"]) == int(run["body"]["run_generation"]) + 1
        assert active["body"]["expires_at_ns"] == run["body"]["expires_at_ns"]
        managed = json.loads((root / "node/managed" / restored["id"] / "managed.json").read_text())
        assert managed["recovery_reference"] == ref
        after_restore = wait(business, 8)
        assert int(after_restore["business_observation"]["data"]["expected_inventory_lots"]) == 1
        restored_pid = int(current()["body"]["observation"]["pid"])
        assert restored_pid != old_pid and not Path(f"/proc/{old_pid}").exists()
        with read_db(checkpoint) as db:
            assert db.execute("SELECT COUNT(*) FROM recovery_pins WHERE record_id=? AND operation_id=?", (record, restored["id"])).fetchone()[0] == 1
            assert db.execute("SELECT payload FROM recovery_points WHERE id=?", (record,)).fetchone()[0] == payload
        passed("new-E-restores-fixed-SQLite-record-position-and-original-expiry")
        venue.replaying = True
        venue.send_trade(venue.trades[0], 3)
        time.sleep(1)
        replay_run = current()["body"]
        (root / "replay-observation.json").write_text(json.dumps({"business": call(f"/api/v2/strategies/{active['body']['strategy_id']}/runs/{active['id']}/snapshot")["business_observation"], "account_risk": replay_run.get("observation", {}).get("account_snapshot", {}).get("image", {}).get("risk")}, indent=2))
        assert {k: v["digest"] for k, v in trades().items()} == opening_digest
        wait(business, 5)
        assert venue.net == Decimal(".0001") and len(venue.orders) == 1
        passed("cross-process-original-trade-replay-preserves-fee-inventory-and-order-count")
        stop_began = time.monotonic()
        submit_stop = control("stop")
        stop_seconds = time.monotonic() - stop_began
        assert stop_seconds <= 10
        wait(lambda: venue.net == 0 and len(trades()) == 2, 5)
        final = trades()
        assert {k: v["digest"] for k, v in final.items() if k in opening_digest} == opening_digest
        assert sorted(int(v["details"]["side"]) for v in final.values()) == [1, 2]
        assert sum(int(v["details"]["fee_amount_units"]) for v in final.values()) == 808001
        assert len(venue.orders) == 2 and len(venue.trades) == 2
        assert all(v["state"] == "FILLED" for v in venue.orders.values())
        exit_order = next(v for v in venue.orders.values() if v["side"] == "SELL")
        assert exit_order["reduce_only"] == "true" and exit_order["time_in_force"] == "IOC"
        stopped = wait(lambda: r if (r := current())["body"]["release_proof"]["safe"] else None, 5)
        wait(lambda: not Path(f"/proc/{restored_pid}").exists(), 5)
        full_stop_seconds = time.monotonic() - stop_began
        assert full_stop_seconds <= 10
        with read_db(root / "manager.sqlite3") as db:
            assert db.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0] == 0
        passed("positioned-reduce-only-Stop-flat-proof-and-reservation-release-within-original-10s")
        old_history, new_history = history(run), history(active)
        assert len(old_history["items"]) == len(new_history["items"]) == 1
        assert int(old_history["recording"]["gap_count"]) > 0
        assert all(v["fact"]["scope"]["environment"] == "simulation" for v in old_history["items"] + new_history["items"])
        with read_db(root / "node/account-facts.sqlite3") as db:
            replay_rows = db.execute("SELECT COUNT(*) FROM archive_records WHERE kind='fill' AND replay=1").fetchone()[0]
            assert replay_rows >= 1 and db.execute("SELECT COUNT(*) FROM archive_conflicts").fetchone()[0] == 0
            issues = [v[0] for v in db.execute("SELECT code FROM issues")]
            assert not {"MISSING_TRADE_ID", "TRADE_ID_CONFLICT", "FEE_CURRENCY_UNAVAILABLE"}.intersection(issues)
        assert len(venue.intent_proofs) == 2 and not venue.failed, venue.failed
        final_point = point()[-1]
        opening_record = next(v for v in final_point["orders_list"] if int(v["side"]) == 1)
        closing_record = next(v for v in final_point["orders_list"] if int(v["side"]) == 2)
        assert int(opening_record["fill_count"]) == int(closing_record["fill_count"]) == 1
        passed("unified-history-deduplicates-native-replay-and-preserves-crash-gap")
        elapsed = time.monotonic() - began
        assert elapsed <= 120
        result = {"passed": True, "candidate": args.candidate_label, "source": "immutable signed artifact through real A/N/E and loopback Gate; no real account",
            "checks": checks, "binary_sha256": args.expected_binary_sha256, "strategy_sha256": args.expected_strategy_sha256,
            "checkpoint_seconds": 60, "handler_seconds": 1, "startup_seconds": 30, "stop_seconds": 10,
            "maximum_run_seconds": 120, "elapsed_seconds": elapsed, "positioned_stop_seconds": stop_seconds,
            "positioned_stop_through_worker_exit_seconds": full_stop_seconds,
            "old_pid": old_pid, "new_pid": restored_pid, "old_generation": run["body"]["run_generation"],
            "new_generation": active["body"]["run_generation"], "old_expires_at_ns": run["body"]["expires_at_ns"],
            "new_expires_at_ns": active["body"]["expires_at_ns"], "reference": ref, "native_fills": final,
            "fee_units_USDT_1e8": "808001", "simulated_fee_policy": "0.0005 quote notional; 1e-8 USDT ROUND_HALF_UP", "cross_process_replay_rows": replay_rows,
            "old_recording": old_history["recording"], "new_recording": new_history["recording"],
            "operations": operations, "durable_intents": venue.intent_proofs, "remaining_mock_position": str(venue.net),
            "remaining_mock_orders": 0, "local_occupancy_rows": 0, "connector_urls": cfg_values,
            "real_host_contacted": False, "real_reservations_touched": False}
        (root / "results.json").write_text(json.dumps(result, indent=2))
        successful = True
    finally:
        for proc in reversed(processes):
            stop(proc)
        remaining = []
        registry = root / "node/runs.sqlite3"
        if registry.exists():
            with read_db(registry) as db:
                owners = db.execute("SELECT pid,process_start_time FROM runs WHERE strategy_id='management-runtime'").fetchall()
            for pid, stamp in owners:
                stat = Path(f"/proc/{pid}/stat")
                if stat.exists() and int(stat.read_text().rsplit(")", 1)[1].split()[19]) == int(stamp):
                    os.kill(pid, signal.SIGTERM)
                    remaining.append(pid)
            deadline = time.monotonic() + 12
            while remaining and time.monotonic() < deadline:
                remaining = [pid for pid in remaining if Path(f"/proc/{pid}").exists()]
                time.sleep(.1)
        (root / "venue.json").write_text(json.dumps({"calls": venue.calls, "failed": venue.failed,
            "ws_frames": venue.ws_frames, "delivery": venue.delivery, "trades": venue.trades, "orders": venue.orders}, indent=2))
        venue.alive = False
        venue.shutdown()
        (secret / "gate.toml").unlink()
        (root / "cleanup.json").write_text(json.dumps({"passed": not remaining, "acceptance_passed": successful,
            "remaining_owned_workers": remaining, "management_processes_exited": all(p.poll() is not None for p in processes),
            "synthetic_credentials_removed": True, "real_host_contacted": False}, indent=2))
        assert not remaining


if __name__ == "__main__":
    main()
