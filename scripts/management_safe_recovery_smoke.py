"""Bounded new-generation recovery after faults, on the build host and loopback only."""
import hashlib
import json
import secrets
import time
from decimal import Decimal
from pathlib import Path
from urllib import parse

from management_fill_fault_smoke import FaultHandler, FaultVenue
from management_fill_recovery_smoke import read_db


class SafeRecoveryHandler(FaultHandler):
    def do_GET(self):
        venue = self.server
        if parse.urlparse(self.path).path == "/api/v4/crossex/history_trades":
            with venue.lock:
                rows = [dict(v) for v in venue.trades] if venue.history_open else []
                if venue.scenario == "missing-history-incomplete":
                    rows = []
                elif venue.scenario == "missing-history-conflict" and rows:
                    rows.append({**rows[0], "fee": "0.00404002"})
                elif venue.scenario == "private-history-fee-conflict" and rows:
                    rows = [{**row, "fee": "0.00404002"} for row in rows] * 3
                else:
                    rows *= 3
                venue.calls.append(("GET", "history_trades"))
                venue.history_reads.append({"at_ns": time.time_ns(), "query": parse.parse_qs(parse.urlparse(self.path).query),
                    "returned_native_ids": [v["transaction_id"] for v in rows], "rows": len(rows), "history_open": venue.history_open})
                self.reply(rows)
            return
        return super().do_GET()


class SafeRecoveryVenue(FaultVenue):
    def __init__(self, checkpoint, scenario):
        super().__init__(checkpoint, scenario)
        self.RequestHandlerClass = SafeRecoveryHandler
        self.history_open = False
        self.history_reads = []


def run_case(c):
    root, venue, args = c["root"], c["venue"], c["args"]
    call, wait, passed, trades = (c[k] for k in ("call", "wait", "passed", "trades"))
    private = args.scenario in ("private-recovery", "private-history-fee-conflict")
    rejection = args.scenario in ("missing-history-incomplete", "missing-history-conflict", "private-history-fee-conflict")
    began = time.monotonic()
    run = c["resource"](c["submit"]({"kind": "start", "deployment_id": c["deployment"]["id"]}), "run")
    c["set_active"](run)
    expiry = run["body"]["expires_at_ns"]

    def occupied():
        with read_db(root / "manager.sqlite3") as db:
            return db.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0]

    def snapshot():
        item = c["current"]()
        return call(f"/api/v2/strategies/{item['body']['strategy_id']}/runs/{item['id']}/snapshot")

    def healthy():
        image = snapshot()
        business = image["business_observation"]
        if business.get("available"):
            assert int(business["data"]["last_error"]) == 0, business["data"]
            account = c["current"]()["body"].get("observation", {}).get("account_snapshot", {}).get("image", {})
            if int(business["data"]["expected_inventory_lots"]) == 1 and account.get("risk", {}).get("complete"):
                return image

    def operate(action, expected):
        key = secrets.token_hex(16)
        item = call("/api/v2/operations", {"idempotency_key": key, "node_id": "live", "action": action})
        duplicate = call("/api/v2/operations", {"idempotency_key": key, "node_id": "live", "action": action})
        assert item["id"] == duplicate["id"]
        final = wait(lambda: v if (v := call("/api/v2/operations/" + item["id"]))["state"] in ("succeeded", "failed", "cancelled") else None, 35)
        assert final["state"] == expected, final
        c["operations"].append({"id": final["id"], "kind": action["kind"], "state": final["state"]})
        return final

    def old_exited():
        with read_db(root / "node/runs.sqlite3") as db:
            owners = db.execute("SELECT pid,process_start_time FROM runs WHERE strategy_id='management-runtime'").fetchall()
        for pid, stamp in owners:
            stat = Path(f"/proc/{pid}/stat")
            if stat.exists() and int(stat.read_text().rsplit(")", 1)[1].split()[19]) == int(stamp):
                return False
        return True

    def read_point(record=None):
        if not c["checkpoint"].exists():
            return None
        with read_db(c["checkpoint"]) as db:
            rows = db.execute("SELECT id,sha256,payload FROM recovery_points ORDER BY rowid DESC").fetchall()
            store, = db.execute("SELECT store_id FROM storage_meta WHERE id=1").fetchone()
        for key, sha, payload in rows:
            point = json.loads(payload)
            if record is not None:
                if key == record:
                    return key, sha, payload, point, store
                continue
            positions = point.get("recovery", {}).get("public", {}).get("positions", [])
            if not point.get("recovery", {}).get("pending_execution", True) and any(
                int(v["side"]) == 1 and int(v["cumulative_filled_lots"]) == 0 for v in point["orders_list"]
            ) and positions and all(int(p["qty_lots"]) == 0 for p in positions):
                assert hashlib.sha256(payload).hexdigest() == sha
                return key, sha, payload, point, store

    wait(lambda: venue.pending and venue.net == Decimal(".0001"), 5)
    if private:
        venue.release_opening()
        wait(lambda: len(trades()) == 1 and healthy(), 6)
        before = {k: v["digest"] for k, v in trades().items()}
        c["control"]("pause")
        selected = wait(lambda: p if (p := c["point"]()) and not p[-1]["recovery"]["pending_execution"]
            and any(int(v["qty_lots"]) == 1 for v in p[-1]["recovery"]["public"]["positions"]) else None, 70)
        key, sha, payload, point = selected
        selected = read_point(key)
        old_sessions = venue.sessions(True)
        venue.disconnect(True)
    else:
        selected = wait(read_point, 5)
        before = {}
        assert not trades()
        abnormal = wait(lambda: s if (s := snapshot())["business_observation"].get("available")
            and int(s["business_observation"]["data"]["last_error"]) == 205 else None, 13)
        (root / "old-abnormal.json").write_text(json.dumps(abnormal, indent=2))
        if not rejection:
            unsafe = wait(lambda: p if (p := c["point"]()) and p[-1]["recovery"]["pending_execution"]
                and time.monotonic() - began >= 60 else None, 65)
            (root / "unsafe-head-proof.json").write_text(json.dumps({"record_id": unsafe[0], "sha256": unsafe[1],
                "pending_execution": True, "selected_for_restore": False, "original_checkpoint_seconds": 60}, indent=2))
        passed("missing-native-fill-latches-205-and-no-financial-fill-is-invented")
    record, sha, payload, point, store = selected
    ref = {"record_id": record, "payload_sha256": sha, "store_id": store, "runtime_strategy_no": 77}
    (root / "selected-point-proof.json").write_text(json.dumps({"reference": ref, "generation": point["generation"],
        "pending_execution": point["recovery"]["pending_execution"], "consumed_lots": 1 if private else 0,
        "original_checkpoint_seconds": 60, "blob_unchanged": True}, indent=2))
    old_stop_at = time.monotonic()
    stopped = operate({"kind": "stop", "run_id": run["id"], "expected_run_generation": run["body"]["run_generation"]}, "failed")
    old_stop_elapsed = time.monotonic() - old_stop_at
    wait(old_exited, 8)
    old = wait(lambda: v if (v := c["current"]())["body"]["process_state"] == "FAILED" else None, 5)
    old_final_path = root / "node/managed" / run["body"]["admission_operation_id"] / "final.json"
    old_final = json.loads(old_final_path.read_text())
    assert not old_final["release_proof"]["safe"] and not old["body"]["release_proof"]["safe"]
    assert all(not s["activation_gate_open"] and not s["command_gate_open"] for s in old_final["strategies"])
    assert occupied() == 2 and venue.net == Decimal(".0001") and len(venue.orders) == 1
    if private:
        assert venue.sessions(True) > old_sessions
    else:
        assert stopped["result"]["runtime"]["detail"] == "stop_drain_deadline_exceeded" and old_stop_elapsed < 14
    (root / "old-closed-proof.json").write_text(json.dumps({"run": old, "stop": stopped,
        "observed_stop_receipt_seconds": old_stop_elapsed, "engine_stop_seconds": 10,
        "old_activation_and_command_gates_closed": True, "local_simulated_occupancy_rows": 2,
        "simulated_position": str(venue.net), "independent_fills": len(trades())}, indent=2))
    passed("old-run-failed-closed-and-exited-before-any-history-backfill-or-new-generation")
    assert (int(expiry) - time.time_ns()) / 1e9 >= 25
    c["stop"](c["managerp"])
    c["stop"](c["nodep"])
    c["start"]("live", "node")
    c["start"]("manager", "manager")
    wait(lambda: call("/api/v2/snapshot"))
    venue.history_open = True
    venue.replaying = True
    restore = operate({"kind": "restore", "run_id": run["id"], "expected_run_generation": run["body"]["run_generation"],
        "recovery_point_sha256": sha}, "failed" if rejection else "succeeded")
    assert venue.history_reads and all(v["history_open"] for v in venue.history_reads)
    (root / "history-backfill-proof.json").write_text(json.dumps({"reads": venue.history_reads, "restore": restore,
        "only_REST_provided_missing_fill": not private, "old_worker_exited_before_queries": True}, indent=2))
    with read_db(c["checkpoint"]) as db:
        assert db.execute("SELECT payload FROM recovery_points WHERE id=?", (record,)).fetchone()[0] == payload
        assert db.execute("SELECT COUNT(*) FROM recovery_pins WHERE record_id=?", (record,)).fetchone()[0] == 1
    if rejection:
        wait(old_exited, 8)
        assert len(trades()) == (1 if private else 0) and occupied() == 2 and len(venue.orders) == 1 and venue.net == Decimal(".0001")
        assert {k: v["digest"] for k, v in trades().items()} == before
        with read_db(c["checkpoint"]) as db:
            generation_floor, = db.execute("SELECT generation FROM strategy_generations WHERE strategy_no=77").fetchone()
        assert int(generation_floor) >= 2
        passed("incomplete-or-conflicting-native-history-rejects-successor-without-trading-or-release")
        result = {"expected_outcome": "restore_rejected", "generation_floor": generation_floor}
    else:
        active = c["resource"](restore, "run")
        c["set_active"](active)
        assert int(active["body"]["run_generation"]) == 2 and active["body"]["expires_at_ns"] == expiry
        wait(lambda: len(trades()) == 1 and healthy(), 8)
        after = {k: v["digest"] for k, v in trades().items()}
        assert all(after[k] == value for k, value in before.items())
        assert sum(int(v["details"]["fee_amount_units"]) for v in trades().values()) == 404001
        assert len(venue.orders) == 1
        opening = next(v for v in c["point"]()[-1]["orders_list"] if int(v["side"]) == 1)
        assert int(opening["cumulative_filled_lots"]) == int(opening["fill_count"]) == 1
        venue.send_trade(venue.trades[0], 3)
        time.sleep(.4)
        assert {k: v["digest"] for k, v in trades().items()} == after
        wait(healthy, 5)
        passed("authoritative-native-backfill-recovers-one-consumption-and-one-fee-from-fixed-SQLite-BLOB")
        stop_at = time.monotonic()
        c["control"]("stop")
        wait(lambda: occupied() == 0 and c["current"]()["body"].get("release_proof", {}).get("safe"), 5)
        wait(old_exited, 5)
        stop_seconds = time.monotonic() - stop_at
        assert stop_seconds <= 10 and venue.net == 0 and len(venue.orders) == 2
        assert len(trades()) == 2 and sum(int(v["details"]["fee_amount_units"]) for v in trades().values()) == 808001
        with read_db(root / "manager.sqlite3") as db:
            retained_old = json.loads(db.execute("SELECT data FROM resources WHERE id=?", (run["id"],)).fetchone()[0])
        assert retained_old["body"]["process_state"] == "FAILED" and not retained_old["body"]["release_proof"]["safe"]
        (root / "terminal-proof.json").write_text(json.dumps({"passed": True, "stop_through_exit_and_release_seconds": stop_seconds,
            "old_run_still_failed_and_unsafe": True, "old_expires_at_ns": expiry, "new_expires_at_ns": active["body"]["expires_at_ns"],
            "fixed_blob_unchanged": True, "fixed_pin_count": 1, "position_lots": 0, "occupancy": 0,
            "independent_fills": 2, "fee_units_USDT_1e8": 808001}, indent=2))
        passed("new-generation-positioned-Stop-flattens-and-releases-within-original-ten-seconds")
        result = {"expected_outcome": "safe_flat_stop", "stop_through_exit_seconds": stop_seconds, "new_generation": "2"}
    assert not venue.failed
    assert time.monotonic() - began <= 120
    return {"passed": True, "scenario": args.scenario, "candidate": args.candidate_label, **result,
        "checks": c["checks"], "operations": c["operations"], "elapsed_seconds": time.monotonic() - began,
        "binary_sha256": args.expected_binary_sha256, "strategy_sha256": args.expected_strategy_sha256,
        "reference": ref, "original_expires_at_ns": expiry, "handler_seconds": 1, "startup_seconds": 30,
        "checkpoint_seconds": 60, "stop_seconds": 10, "maximum_run_seconds": 120, "local_simulated_occupancy_rows": occupied(),
        "old_run_not_reopened": True, "connector_urls": c["connector_urls"], "real_host_contacted": False, "real_reservations_touched": False}
