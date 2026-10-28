"""Fault scenarios for the build-host-only A/N/E loopback harness; never a live runner."""
import hashlib
import json
import os
import secrets
import signal
import socket
import time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from urllib import error

from management_fill_recovery_smoke import FillHandler, FillVenue, SYMBOL, read_db


class FaultHandler(FillHandler):
    def do_GET(self):
        venue = self.server
        if self.path.startswith("/api/v4/crossex/accounts") and venue.bootstrap_armed and not venue.bootstrap_injected:
            with venue.lock:
                venue.bootstrap_injected = True
                venue.replaying = True
                venue.events.append({"kind": "bootstrap-replay-before-REST-response", "trades": len(venue.trades),
                                     "private_sessions": venue.sessions(True)})
                for trade in venue.trades:
                    venue.send_trade(trade, 3)
            # REST cannot complete its bootstrap while these already-written WS facts arrive.
            time.sleep(.25)
        return super().do_GET()


class FaultVenue(FillVenue):
    def __init__(self, checkpoint, scenario):
        super().__init__(checkpoint)
        self.RequestHandlerClass = FaultHandler
        self.scenario = scenario
        self.events = []
        self.partial_order = None
        self.bootstrap_armed = self.bootstrap_injected = False
        self.market_session_before_fault = None

    def sessions(self, private):
        return sum(v.get("path") == ("/private" if private else "/public") for v in self.ws_frames)

    def disconnect(self, private):
        with self.lock:
            streams = list(self.private if private else self.public)
            (self.private if private else self.public).clear()
            self.events.append({"kind": "disconnect", "private": private, "sessions": self.sessions(private)})
        for stream in streams:
            try:
                stream.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        assert streams, "fault must sever an established owned socket"

    def fill(self, body):
        if self.scenario == "market-reconnect" and body["side"] == "SELL":
            until = time.monotonic() + 4
            while self.sessions(False) <= self.market_session_before_fault and time.monotonic() < until:
                time.sleep(.02)
            assert self.sessions(False) > self.market_session_before_fault, "market socket never reconnected"
        if self.scenario != "partial":
            return super().fill(body)
        with self.lock:
            self.durable_intent(body["text"])
            assert body["symbol"] == SYMBOL and body["type"] == "LIMIT" and body["time_in_force"] == "IOC"
            assert Decimal(body["qty"]) == Decimal(".0002") and len(self.orders) < 2
            opening = body["side"] == "BUY"
            assert opening == (len(self.orders) == 0)
            assert body["reduce_only"] == ("false" if opening else "true")
            assert self.net == (Decimal(0) if opening else Decimal(".0002"))
            assert Decimal(body["price"]) * Decimal(body["qty"]) < 100
            now = self.now()
            key = str(2072784922592768 + len(self.orders))
            order = {**body, "order_id": key, "state": "NEW", "executed_qty": "0", "executed_avg_price": "0",
                     "create_time": now, "update_time": now, "position_side": "NONE"}
            self.orders[key] = order
            self.broadcast("order", dict(order))
            self._part(order, opening, final=False, defer=opening)
            if opening:
                self.partial_order = order
            else:
                # Two executions of one IOC; stagger delivery to exercise partial projections.
                import threading
                timer = threading.Timer(.25, self._finish_close, args=(order,))
                timer.daemon = True
                timer.start()
            return dict(order)

    def _part(self, order, opening, final, defer=False):
        qty = Decimal(".0001")
        price = Decimal("80800.1" if opening else "80800.0")
        assert Decimal(order["price"]) >= price if opening else Decimal(order["price"]) <= price
        fee = (qty * price * Decimal(".0005")).quantize(Decimal(".00000001"), rounding=ROUND_HALF_UP)
        pnl = Decimal(0) if opening else qty * (price - self.entry)
        self.wallet += pnl - fee
        self.net += qty if opening else -qty
        self.entry = price if opening else (self.entry if self.net else Decimal(0))
        self.now()
        self.broadcast("position", self.position())
        cumulative = Decimal(order["executed_qty"]) + qty
        order.update(state="FILLED" if final else "PARTIALLY_FILLED", executed_qty=str(cumulative),
                     executed_avg_price=str(price), last_executed_qty=str(qty), last_executed_price=str(price),
                     fee_coin="USDT", fee=str(fee), update_time=self.now())
        self.broadcast("order", dict(order))
        trade = {"symbol": SYMBOL, "transaction_id": str(2072784922594048 + len(self.trades)),
                 "order_id": order["order_id"], "text": order["text"], "side": order["side"], "qty": str(qty),
                 "price": str(price), "fee": str(fee), "fee_coin": "USDT", "fee_rate": "0.0005", "rpnl": str(pnl),
                 "match_role": "TAKER", "position_mode": "SINGLE", "position_side": "NONE", "create_time": self.now()}
        self.trades.append(trade)
        self.events.append({"kind": "partial-execution", "side": order["side"], "last_qty": str(qty),
                            "cumulative_qty": str(cumulative), "state": order["state"]})
        if defer:
            self.pending.append(trade)
        else:
            self.send_trade(trade, 3)

    def finish_opening(self):
        with self.lock:
            assert self.partial_order and len(self.trades) == 1
            self._part(self.partial_order, True, final=True)

    def _finish_close(self, order):
        with self.lock:
            try:
                self._part(order, False, final=True)
            except Exception as exc:
                self.failed.append(("partial-close", repr(exc)))


def run_case(c):
    root, venue, args = c["root"], c["venue"], c["args"]
    scenario = args.scenario
    call, wait, passed, trades = (c[k] for k in ("call", "wait", "passed", "trades"))
    started = time.monotonic()
    run = c["resource"](c["submit"]({"kind": "start", "deployment_id": c["deployment"]["id"]}), "run")
    c["set_active"](run)
    active = run
    initial_expiry = run["body"]["expires_at_ns"]
    fault_started = None
    lots = 2 if scenario == "partial" else 1
    fee_before = None
    selected_ref = None

    def image():
        item = c["current"]()
        if not item:
            return None
        return call(f"/api/v2/strategies/{item['body']['strategy_id']}/runs/{item['id']}/snapshot")

    def healthy(quantity):
        snap = image()
        if not snap:
            return None
        obs = snap["business_observation"]
        if obs.get("available"):
            assert int(obs["data"]["last_error"]) == 0, obs["data"]
            account = c["current"]()["body"].get("observation", {}).get("account_snapshot", {}).get("image", {})
            if int(obs["data"]["expected_inventory_lots"]) == quantity and account.get("risk", {}).get("complete"):
                return snap

    def occupancy():
        with read_db(root / "manager.sqlite3") as db:
            return db.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0]

    def durable_rows(kind):
        with read_db(root / "node/account-facts.sqlite3") as db:
            return [json.loads(v[0]) for v in db.execute("SELECT body FROM facts WHERE kind=?", (kind,))]

    def capture(name):
        snap = image()
        run_image = c["current"]()
        (root / (name + ".json")).write_text(json.dumps({"snapshot": snap, "run": run_image}, indent=2))
        return snap

    def wait_exit():
        registry = root / "node/runs.sqlite3"
        with read_db(registry) as db:
            owners = db.execute("SELECT pid,process_start_time FROM runs WHERE strategy_id='management-runtime'").fetchall()
        def exited():
            for pid, stamp in owners:
                stat = Path(f"/proc/{pid}/stat")
                if stat.exists() and int(stat.read_text().rsplit(")", 1)[1].split()[19]) == int(stamp):
                    return False
            return True
        wait(exited, 8)

    wait(lambda: venue.pending and venue.net == Decimal(".0001"), 5)
    if scenario != "abnormal-stop":
        venue.release_opening()
        wait(lambda: len(trades()) == 1, 5)
        wait(lambda: healthy(1), 5)
        if scenario == "partial":
            partial_rows = durable_rows("order")
            assert any(int(v["details"]["quantity_lots"]) == 2 and int(v["details"]["filled_quantity_lots"]) == 1 for v in partial_rows)
            assert len(venue.orders) == 1 and venue.net == Decimal(".0001")
            passed("partial-one-lot-consumed-with-one-lot-still-unfilled-no-extra-opening")
            venue.finish_opening()
            wait(lambda: len(trades()) == 2, 5)
            wait(lambda: healthy(2), 5)
            passed("two-native-trade-ids-advance-partial-cumulative-quantity-and-fees-once")
        fee_before = {k: v["digest"] for k, v in trades().items()}
    else:
        assert not trades()
        passed("withheld-native-fill-leaves-position-order-and-strategy-inventory-unsettled")

    if scenario in ("partial", "bootstrap-replay"):
        c["control"]("pause")
        def eligible():
            selected = c["point"]()
            if not selected or selected[-1]["recovery"]["pending_execution"]:
                return None
            positions = selected[-1]["recovery"]["public"]["positions"]
            return selected if any(int(v["qty_lots"]) == lots for v in positions) else None
        selected = wait(eligible, 70)
        record, sha, payload, point = selected
        wait(lambda: healthy(lots), 5)
        assert (int(initial_expiry) - time.time_ns()) / 1e9 >= 40
        observed = wait(lambda: r if (r := c["current"]())["body"].get("recovery_reference", {}).get("record_id") == record else None, 5)
        selected_ref = observed["body"]["recovery_reference"]
        (root / "checkpoint-proof.json").write_text(json.dumps({"reference": selected_ref, "position_lots": lots,
            "pending_execution": False, "checkpoint_seconds": 60, "elapsed_seconds": time.monotonic() - started}, indent=2))
        obs = observed["body"]["observation"]
        pid, stamp = int(obs["pid"]), int(obs["start_time"])
        assert int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]) == stamp
        os.kill(pid, signal.SIGKILL)
        wait(lambda: c["current"]()["body"]["process_state"] == "UNKNOWN")
        assert occupancy() == 2
        c["stop"](c["managerp"])
        c["stop"](c["nodep"])
        c["start"]("live", "node")
        c["start"]("manager", "manager")
        wait(lambda: call("/api/v2/snapshot"))
        venue.bootstrap_armed = scenario == "bootstrap-replay"
        restored = c["submit"]({"kind": "restore", "run_id": run["id"], "expected_run_generation": run["body"]["run_generation"], "recovery_point_sha256": sha})
        active = c["resource"](restored, "run")
        c["set_active"](active)
        assert int(active["body"]["run_generation"]) == int(run["body"]["run_generation"]) + 1
        assert active["body"]["expires_at_ns"] == initial_expiry
        wait(lambda: healthy(lots), 8)
        if scenario == "bootstrap-replay":
            assert venue.bootstrap_injected
            fills = [v for v in durable_rows("fill") if v["source"].startswith(restored["id"] + ":")]
            ready = [v for v in durable_rows("stream") if v["source"].startswith(restored["id"] + ":") and int(v["details"]["state"]) == 4]
            assert fills and ready and min(int(v["version"]) for v in fills) < min(int(v["version"]) for v in ready)
            (root / "bootstrap-proof.json").write_text(json.dumps({"passed": True, "replay_fill_versions": [v["version"] for v in fills],
                "ready_versions": [v["version"] for v in ready], "injected_before_REST_response": True}, indent=2))
            passed("bootstrap-buffered-native-replay-precedes-READY-without-double-inventory-or-fees")
        venue.replaying = True
        for trade in venue.trades[:lots]:
            venue.send_trade(trade, 3)
        time.sleep(.4)
        assert {k: v["digest"] for k, v in trades().items()} == fee_before
        wait(lambda: healthy(lots), 5)
        with read_db(c["checkpoint"]) as db:
            assert db.execute("SELECT payload FROM recovery_points WHERE id=?", (record,)).fetchone()[0] == payload
            assert db.execute("SELECT COUNT(*) FROM recovery_pins WHERE record_id=?", (record,)).fetchone()[0] == 1
        passed("fixed-SQLite-cross-process-recovery-replays-preserve-consumed-partial-boundaries-and-original-expiry")

    expected_failed_stop = scenario in ("abnormal-stop", "private-disconnect")
    if scenario == "abnormal-stop":
        def abnormal():
            snap = image()
            obs = (snap or {}).get("business_observation", {})
            return snap if obs.get("available") and int(obs["data"]["last_error"]) != 0 and obs["data"]["phase"] in ("error", "reconciling") else None
        wait(abnormal, 13)
        capture("abnormal-before-stop")
        passed("original-strategy-disagreement-deadline-latches-abnormal-state-without-inventing-a-fill")
    if scenario in ("market-reconnect", "private-disconnect"):
        venue.market_session_before_fault = venue.sessions(False)
        private_sessions = venue.sessions(True)
        fault_started = time.monotonic()
        venue.disconnect(scenario == "private-disconnect")

    stop_began = time.monotonic()
    action = {"kind": "stop", "run_id": active["id"], "expected_run_generation": active["body"]["run_generation"]}
    key = secrets.token_hex(16)
    try:
        op = call("/api/v2/operations", {"idempotency_key": key, "node_id": "live", "action": action})
        repeated = call("/api/v2/operations", {"idempotency_key": key, "node_id": "live", "action": action})
        assert op["id"] == repeated["id"]
        outcome = wait(lambda: v if (v := call("/api/v2/operations/" + op["id"]))["state"] in ("succeeded", "failed", "cancelled") else None, 15)
        c["operations"].append({"id": outcome["id"], "kind": "stop", "state": outcome["state"]})
    except error.HTTPError as exc:
        assert expected_failed_stop and exc.code in (409, 422)
        outcome = {"state": "rejected", "http_status": exc.code}
    observed_stop_seconds = time.monotonic() - stop_began
    (root / "stop-outcome.json").write_text(json.dumps(outcome, indent=2))
    assert active["body"]["expires_at_ns"] == initial_expiry

    if expected_failed_stop:
        assert outcome["state"] in ("failed", "cancelled", "rejected"), outcome
        wait_exit()
        terminal = wait(lambda: v if (v := c["current"]())["body"]["process_state"] in ("FAILED", "UNKNOWN", "STOPPED") else None, 5)
        assert not terminal["body"].get("release_proof", {}).get("safe")
        (root / "manager-terminal-proof.json").write_text(json.dumps({"passed": True, "run": terminal,
            "local_simulated_occupancy_rows": occupancy()}, indent=2))
        assert venue.net == Decimal(".0001") and len(venue.orders) == 1
        assert occupancy() == 2
        final_file = root / "node/managed" / active["body"]["admission_operation_id"] / "final.json"
        final = json.loads(final_file.read_text())
        assert not final.get("release_proof", {}).get("safe", False)
        if scenario == "abnormal-stop":
            assert not trades()
            receipt = outcome["result"]["runtime"]
            assert receipt["input"]["action"] == "stop" and receipt["input"]["id"] == op["id"]
            assert receipt["state"] == "failed" and receipt["runtime_operation_id"]
            assert receipt["detail"] == "stop_drain_deadline_exceeded", receipt
            # A timeout result is observed after the fixed 10 s engine deadline; the 15 s
            # observer above is transport/polling allowance, never a changed Stop deadline.
            assert observed_stop_seconds < 13
            (root / "stop-timeout-proof.json").write_text(json.dumps({"engine_stop_budget_seconds": 10,
                "observed_receipt_seconds": observed_stop_seconds, "runtime_receipt": receipt,
                "duplicate_request_same_operation": True, "original_run_expiry_unchanged": True}, indent=2))
        else:
            assert venue.sessions(True) > private_sessions
            assert {k: v["digest"] for k, v in trades().items()} == fee_before
        passed("abnormal-Stop-fails-closed-at-original-budget-retains-position-and-reservations")
    else:
        assert outcome["state"] == "succeeded", outcome
        wait(lambda: occupancy() == 0 and c["current"]()["body"].get("release_proof", {}).get("safe"), 5)
        wait_exit()
        full_stop_seconds = time.monotonic() - stop_began
        assert full_stop_seconds <= 10
        if scenario == "market-reconnect":
            assert venue.sessions(False) > venue.market_session_before_fault
            assert time.monotonic() - fault_started <= 10
            passed("public-socket-reconnects-and-protective-exit-completes-before-original-Stop-deadline")
        assert venue.net == 0 and len(venue.orders) == 2 and occupancy() == 0
        final_trades = trades()
        assert len(final_trades) == 2 * lots
        assert {k: v["digest"] for k, v in final_trades.items() if k in fee_before} == fee_before
        expected_fee = 808001 * lots
        assert sum(int(v["details"]["fee_amount_units"]) for v in final_trades.values()) == expected_fee
        head = c["point"]()[-1]
        records = [{k: v[k] for k in ("order_id", "side", "cumulative_filled_lots", "fill_count")} for v in head["orders_list"]]
        assert len(records) == 2 and all(int(v["fill_count"]) == lots and int(v["cumulative_filled_lots"]) == lots for v in records)
        (root / "terminal-proof.json").write_text(json.dumps({"passed": True, "stop_through_exit_seconds": full_stop_seconds,
            "orders": records, "unique_native_fills": len(final_trades), "fee_units_USDT_1e8": expected_fee,
            "position_lots": 0, "local_simulated_occupancy_rows": 0}, indent=2))
        passed("trusted-facts-Stop-flattens-with-reduce-only-and-releases-only-local-simulated-occupancy")
    assert not venue.failed, venue.failed
    assert time.monotonic() - started <= 120
    (root / "fault-events.json").write_text(json.dumps({"scenario": scenario, "events": venue.events,
        "private_sessions": venue.sessions(True), "public_sessions": venue.sessions(False)}, indent=2))
    return {"passed": True, "scenario": scenario, "expected_runtime_outcome": "failed_closed" if expected_failed_stop else "safe_flat_stop",
        "candidate": args.candidate_label, "binary_sha256": args.expected_binary_sha256, "strategy_sha256": args.expected_strategy_sha256,
        "checks": c["checks"], "operations": c["operations"], "elapsed_seconds": time.monotonic() - started,
        "observed_stop_receipt_seconds": observed_stop_seconds, "engine_stop_budget_seconds": 10, "checkpoint_seconds": 60,
        "startup_seconds": 30, "handler_seconds": 1, "maximum_run_seconds": 120, "old_expires_at_ns": initial_expiry,
        "new_expires_at_ns": active["body"]["expires_at_ns"], "old_generation": run["body"]["run_generation"],
        "new_generation": active["body"]["run_generation"], "reference": selected_ref, "simulated_position_base": str(venue.net),
        "local_simulated_occupancy_rows": occupancy(), "independent_fill_rows": len(trades()), "connector_urls": c["connector_urls"],
        "real_host_contacted": False, "real_reservations_touched": False}
