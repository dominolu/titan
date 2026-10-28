#!/usr/bin/env python3
"""Independent read-only SQLite audit of safe recovery after account faults."""
import argparse
import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path


def read(path):
    return json.loads(path.read_text())


def db(path):
    return sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True)


def audit(root):
    result, cleanup = (read(root / name) for name in ("results.json", "cleanup.json"))
    assert result["passed"] and cleanup["passed"] and cleanup["management_processes_exited"]
    assert cleanup["synthetic_credentials_removed"] and not cleanup["remaining_owned_workers"]
    assert result["old_run_not_reopened"] and not result["real_host_contacted"] and not result["real_reservations_touched"]
    rejected = result["expected_outcome"] == "restore_rejected"
    fee_conflict = result["scenario"] == "private-history-fee-conflict"
    selected, closed, backfill = (read(root / name) for name in ("selected-point-proof.json", "old-closed-proof.json", "history-backfill-proof.json"))
    assert not selected["pending_execution"] and closed["old_activation_and_command_gates_closed"]
    assert closed["run"]["body"]["process_state"] == "FAILED" and not closed["run"]["body"]["release_proof"]["safe"]
    assert backfill["old_worker_exited_before_queries"] and backfill["reads"]
    with db(root / "manager.sqlite3") as conn:
        occupied, = conn.execute("SELECT COUNT(*) FROM occupancy").fetchone()
        runs = [json.loads(v[0]) for v in conn.execute("SELECT data FROM resources WHERE kind='run'")]
    assert occupied == (2 if rejected else 0)
    assert all(v["body"]["expires_at_ns"] == result["original_expires_at_ns"] for v in runs)
    original = next(v for v in runs if v["id"] == closed["run"]["id"])
    assert original["body"]["process_state"] == "FAILED" and not original["body"]["release_proof"]["safe"]
    budgets = [read(p)["budget"] for p in (root / "node/managed").glob("*/managed.json")]
    assert budgets and all(v == budgets[0] for v in budgets)
    assert budgets[0]["expires_at_ns"] == result["original_expires_at_ns"]
    old_final = read(root / "node/managed" / original["body"]["admission_operation_id"] / "final.json")
    old_gates = [{k: s[k] for k in ("activation_gate_open", "command_gate_open", "lifecycle", "last_error_code")} for s in old_final["strategies"]]
    assert old_gates and all(not s["activation_gate_open"] and not s["command_gate_open"] for s in old_gates)
    with db(root / "node/account-facts.sqlite3") as conn:
        fills = [json.loads(v[0]) for v in conn.execute("SELECT body FROM trades")]
        assert len(fills) == (1 if fee_conflict else 0 if rejected else 2)
        fees = sum(int(v["details"]["fee_amount_units"]) for v in fills)
        assert fees == (404001 if fee_conflict else 0 if rejected else 808001)
        conflicts, = conn.execute("SELECT COUNT(*) FROM archive_conflicts").fetchone()
        assert conflicts > 0 if fee_conflict else conflicts == 0
        assert not conn.execute("SELECT COUNT(*) FROM archive_records WHERE environment<>'simulation'").fetchone()[0]
        for payload, sha in conn.execute("SELECT payload,sha256 FROM archive_records"):
            assert hashlib.sha256(payload).hexdigest() == sha
        issues = [v[0] for v in conn.execute("SELECT code FROM issues")]
        assert ("TRADE_ID_CONFLICT" in issues) == fee_conflict
        if result["scenario"] in ("private-recovery", "private-history-fee-conflict"):
            assert "ACCOUNT_STREAM_GAP" in issues
        archive_fills = conn.execute("SELECT replay,COUNT(*) FROM archive_records WHERE kind='fill' GROUP BY replay").fetchall()
        sources = conn.execute("SELECT recording,clean,complete FROM archive_sources").fetchall()
    checkpoint = root / "node/checkpoints/simulation-candidate04-gate-1/77/runtime.db"
    with db(checkpoint) as conn:
        ref = result["reference"]
        sha, payload = conn.execute("SELECT sha256,payload FROM recovery_points WHERE id=?", (ref["record_id"],)).fetchone()
        assert hashlib.sha256(payload).hexdigest() == sha == ref["payload_sha256"]
        pin_count, = conn.execute("SELECT COUNT(*) FROM recovery_pins WHERE record_id=?", (ref["record_id"],)).fetchone()
        assert pin_count == 1
        floor, = conn.execute("SELECT generation FROM strategy_generations WHERE strategy_no=77").fetchone()
        assert int(floor) >= 2
        final_payload, = conn.execute("SELECT p.payload FROM recovery_heads h JOIN recovery_points p ON p.id=h.record_id WHERE h.strategy_no=77").fetchone()
        final_point = json.loads(final_payload)
    terminal = None
    if not rejected:
        terminal = read(root / "terminal-proof.json")
        assert terminal["passed"] and terminal["stop_through_exit_and_release_seconds"] <= 10
        assert terminal["old_expires_at_ns"] == terminal["new_expires_at_ns"]
        assert int(final_point["generation"]) == 2 and not final_point["recovery"]["pending_execution"]
        assert all(int(p["qty_lots"]) == 0 for p in final_point["recovery"]["public"]["positions"])
        assert len(final_point["orders_list"]) == 2
        assert all(int(v["fill_count"]) == int(v["cumulative_filled_lots"]) == 1 for v in final_point["orders_list"])
        successor = next(v for v in runs if int(v["body"]["run_generation"]) == 2)
        assert successor["body"]["process_state"] == "STOPPED" and successor["body"]["release_proof"]["safe"]
    proof = {"passed": True, "scenario": result["scenario"], "expected_outcome": result["expected_outcome"],
        "independent_fills": len(fills), "fee_units_USDT_1e8": fees, "archive_fill_counts_by_replay": archive_fills,
        "archive_conflicts": conflicts, "issues_preserved": issues, "sources": sources, "fixed_reference": ref,
        "fixed_record_sha_valid": True, "fixed_pin_count": pin_count, "generation_floor": floor,
        "original_expiry_unchanged": True, "original_run_still_failed_and_unsafe": True,
        "original_monotonic_budget_unchanged": budgets[0], "old_terminal_gates": old_gates,
        "local_simulated_occupancy_rows": occupied, "final_point_generation": final_point["generation"],
        "final_point_orders": [{k: v[k] for k in ("order_id", "side", "cumulative_filled_lots", "fill_count")} for v in final_point["orders_list"]],
        "terminal": terminal, "real_host_contacted": False, "real_reservations_touched": False}
    (root / "audit.json").write_text(json.dumps(proof, indent=2))
    return proof


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cases", type=Path, nargs="+")
    args = parser.parse_args()
    assert "192.168.3.88" in subprocess.check_output(["hostname", "-I"]).decode()
    for root in args.cases:
        proof = audit(root.resolve())
        print(proof["scenario"], proof["expected_outcome"], "audit passed")


if __name__ == "__main__":
    main()
