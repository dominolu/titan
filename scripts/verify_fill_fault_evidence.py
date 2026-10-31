#!/usr/bin/env python3
"""Read-only audit of the build-host loopback fault matrix, after all workers exit."""
import argparse
import hashlib
import json
import sqlite3
import subprocess
from decimal import Decimal
from pathlib import Path


def digest(path):
    return hashlib.file_digest(path.open("rb"), "sha256").hexdigest()


def read(path):
    return json.loads(path.read_text())


def db(path):
    return sqlite3.connect("file:" + str(path) + "?mode=ro", uri=True)


def audit(root):
    result, cleanup, venue = (read(root / name) for name in ("results.json", "cleanup.json", "venue.json"))
    assert result["passed"] and cleanup["passed"] and cleanup["management_processes_exited"]
    assert cleanup["synthetic_credentials_removed"] and not cleanup["remaining_owned_workers"]
    assert not venue["failed"] and not result["real_host_contacted"] and not result["real_reservations_touched"]
    for order in venue["orders"].values():
        assert abs(Decimal(order["price"]) - Decimal("80800.05")) / Decimal("80800.05") <= Decimal(".005")
        assert Decimal(order["price"]) * Decimal(order["qty"]) < 100
    assert result["old_expires_at_ns"] == result["new_expires_at_ns"]
    scenario = result["scenario"]
    negative = result["expected_runtime_outcome"] == "failed_closed"
    if scenario == "abnormal-stop":
        timeout = read(root / "stop-timeout-proof.json")
        assert timeout["engine_stop_budget_seconds"] == 10
        assert 10 <= timeout["observed_receipt_seconds"] < 13
        assert timeout["runtime_receipt"]["detail"] == "stop_drain_deadline_exceeded"
    lots = 2 if scenario == "partial" else 1
    expected_fills = 0 if scenario == "abnormal-stop" else 1 if negative else lots * 2
    expected_fee = 0 if scenario == "abnormal-stop" else 404001 if negative else 808001 * lots
    with db(root / "node/account-facts.sqlite3") as conn:
        fills = [json.loads(v[0]) for v in conn.execute("SELECT body FROM trades")]
        assert len(fills) == expected_fills
        assert sum(int(v["details"]["fee_amount_units"]) for v in fills) == expected_fee
        assert conn.execute("SELECT COUNT(*) FROM archive_conflicts").fetchone()[0] == 0
        assert not conn.execute("SELECT COUNT(*) FROM archive_records WHERE environment<>'simulation'").fetchone()[0]
        for payload, sha in conn.execute("SELECT payload,sha256 FROM archive_records"):
            assert hashlib.sha256(payload).hexdigest() == sha
        counts = conn.execute("SELECT replay,COUNT(*) FROM archive_records WHERE kind='fill' GROUP BY replay").fetchall()
        assert dict(counts).get(0, 0) == expected_fills
        assert dict(counts).get(1, 0) == (lots if scenario in ("partial", "bootstrap-replay") else 0)
        sources = conn.execute("SELECT recording,clean,complete FROM archive_sources").fetchall()
        gaps = conn.execute("SELECT recording,code FROM archive_gaps").fetchall()
        issues = [v[0] for v in conn.execute("SELECT code FROM issues")]
        assert not {"MISSING_TRADE_ID", "TRADE_ID_CONFLICT", "FEE_CURRENCY_UNAVAILABLE"}.intersection(issues)
        if scenario == "private-disconnect":
            assert "ACCOUNT_STREAM_GAP" in issues
        max_exposure = [json.loads(v[0]) for v in conn.execute("SELECT body FROM risk_windows")]
        assert all(Decimal(v["max_pending_bound_notional"]) <= 100 and Decimal(v["max_actual_notional"]) <= 100 for v in max_exposure)
    with db(root / "manager.sqlite3") as conn:
        occupied = conn.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0]
        runs = [json.loads(v[0])["body"] for v in conn.execute("SELECT data FROM resources WHERE kind='run'")]
    assert all(v["expires_at_ns"] == result["old_expires_at_ns"] for v in runs)
    latest = max(runs, key=lambda v: int(v["run_generation"]))
    assert latest["release_proof"]["safe"] == (not negative)
    if negative:
        assert latest["process_state"] in ("FAILED", "UNKNOWN", "STOPPED")
    assert occupied == (2 if negative else 0)
    checkpoint = root / "node/checkpoints/simulation-candidate04-gate-1/77/runtime.db"
    with db(checkpoint) as conn:
        payload, = conn.execute("SELECT p.payload FROM recovery_heads h JOIN recovery_points p ON p.id=h.record_id").fetchone()
        point = json.loads(payload)
        ref = result["reference"]
        pin_count = None
        if ref:
            sha, original = conn.execute("SELECT sha256,payload FROM recovery_points WHERE id=?", (ref["record_id"],)).fetchone()
            assert hashlib.sha256(original).hexdigest() == sha == ref["payload_sha256"]
            pin_count = conn.execute("SELECT COUNT(*) FROM recovery_pins WHERE record_id=?", (ref["record_id"],)).fetchone()[0]
            assert pin_count == 1
    orders = [{k: v[k] for k in ("order_id", "side", "cumulative_filled_lots", "fill_count")} for v in point["orders_list"]]
    if not negative:
        assert len(orders) == 2 and all(int(v["fill_count"]) == lots and int(v["cumulative_filled_lots"]) == lots for v in orders)
        assert not point["recovery"]["pending_execution"]
    proof = {"passed": True, "scenario": scenario, "expected_runtime_outcome": result["expected_runtime_outcome"],
             "independent_fills": len(fills), "fee_units_USDT_1e8": expected_fee, "archive_fill_counts_by_replay": counts,
             "archive_conflicts": 0, "archive_sources": sources, "archive_gaps": gaps, "financial_issues": issues,
             "local_simulated_occupancy_rows": occupied, "final_point_generation": point["generation"],
             "manager_terminal_process_state": latest["process_state"], "manager_terminal_release_safe": latest["release_proof"]["safe"],
             "all_manager_run_expiries_match_original": True,
             "final_point_orders": orders, "fixed_record_pin_count": pin_count, "sampled_risk": max_exposure,
             "real_host_contacted": False, "real_reservations_touched": False}
    (root / "audit.json").write_text(json.dumps(proof, indent=2))
    return proof


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo", type=Path, required=True)
    p.add_argument("--matrix", type=Path, required=True)
    p.add_argument("--partial-artifact", type=Path, required=True)
    p.add_argument("--abnormal-case", type=Path)
    p.add_argument("--private-case", type=Path)
    args = p.parse_args()
    assert "192.168.3.88" in subprocess.check_output(["hostname", "-I"]).decode()
    repo = args.repo.resolve()
    release = repo / "evidence/m4-fill-release-05"
    package = read(release / "release-manifest.json")
    assert all(digest(release / n) == v["sha256"] for n, v in package["files"].items())
    frozen = read(release / "verification/source-identity.json")["source_sha256"]
    changed = {n: {"before": sha, "after": digest(repo / n)} for n, sha in frozen.items() if digest(repo / n) != sha}
    assert set(changed) <= {"scripts/management_fill_recovery_smoke.py"}, changed
    partial = read(args.partial_artifact / "strategy_version-source/manifest.json")
    differences = {k: {"before": v, "after": partial["parameters"][k]} for k, v in package["parameters"].items() if partial["parameters"][k] != v}
    assert differences == {"order_qty_lots": {"before": 1, "after": 2}, "max_position_lots": {"before": 1, "after": 2}}
    paths = {name: args.matrix / name for name in ("partial", "bootstrap-replay", "market-reconnect", "private-disconnect", "abnormal-stop")}
    if args.abnormal_case:
        paths["abnormal-stop"] = args.abnormal_case
    if args.private_case:
        paths["private-disconnect"] = args.private_case
    cases = [audit(path) for path in paths.values()]
    proof = {"passed": True, "build_host": "192.168.3.88", "candidate": "candidate05", "binary_sha256": package["binary_sha256"],
             "original_strategy_sha256": package["strategy_file_sha256"], "partial_strategy_sha256": partial["strategy_file_sha256"],
             "frozen_release_files_unchanged": len(package["files"]), "frozen_source_files": len(frozen),
             "original_source_changes": changed, "product_source_unchanged": True, "partial_parameter_differences": differences,
             "strategy_source_sha256": digest(repo / "strategies/grid.py"), "cases": [v["scenario"] for v in cases],
             "case_paths": {k: str(v.resolve()) for k, v in paths.items()},
             "test_source_sha256": {n: digest(repo / n) for n in ("scripts/management_fill_recovery_smoke.py", "scripts/management_fill_fault_smoke.py", "scripts/verify_fill_fault_evidence.py", "configs/sim-partial-recovery-acceptance.parameters.json")},
             "real_host_contacted": False, "real_reservations_touched": False}
    (args.matrix / "matrix-audit.json").write_text(json.dumps(proof, indent=2))
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
