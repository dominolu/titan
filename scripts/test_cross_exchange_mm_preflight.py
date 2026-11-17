"""Unit tests for the static cross_exchange_mm configuration preflight."""

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cross_exchange_mm_preflight as preflight  # noqa: E402

TIMER = "timer_interval = { secs = 0, nanos = 100000000 },"

# One maker lot is 0.01 base BTC, one hedge lot is 0.001 base BTC: ratio 10.
BASE = """
schema_version = 1

[market_service]
max_sources = 16
max_instruments = 4096
stop_timeout_ms = 15000

[account_service]
max_accounts = 32
max_instruments_per_account = 4096
max_currencies_per_account = 512
secret_root = "secrets"
startup_timeout_ms = 30000
stop_timeout_ms = 15000

[strategy_service]
allowed_artifact_roots = ["artifacts"]
checkpoint_root = "checkpoints"

[[market_sources]]
source_key = "maker"
connector_type = "okx"
instruments = [{ native_symbol = "BTC-USDT-SWAP", asset_id = 1001, price_tick = { coefficient = 1, scale = 1 }, quantity_lot = { coefficient = 1, scale = 2 }, contract_multiplier = { coefficient = 1, scale = 0 } }]
enabled = true
definition_version = 1

[[market_sources]]
source_key = "hedge"
connector_type = "binance-futures"
instruments = [{ native_symbol = "BTCUSDT", asset_id = 1002, price_tick = { coefficient = 1, scale = 1 }, quantity_lot = { coefficient = 1, scale = 3 }, contract_multiplier = { coefficient = 1, scale = 0 } }]
enabled = true
definition_version = 1

[[accounts]]
account_key = "maker"
account_id = 2001
connector_type = "okx-account"
credential_ref = "secret://file/maker.toml"
instruments = [{ native_symbol = "BTC-USDT-SWAP", asset_id = 1001, price_tick = { coefficient = 1, scale = 1 }, quantity_lot = { coefficient = 1, scale = 2 }, contract_multiplier = { coefficient = 1, scale = 0 } }]
currencies = [{ native_currency = "USDT", currency_id = 1, amount_unit = { coefficient = 1, scale = 8 } }]
ownership = "ObserveAll"
shutdown_order_policy = "CancelAll"
enabled = true
definition_version = 1

[[accounts]]
account_key = "hedge"
account_id = 2002
connector_type = "binance-futures-account"
credential_ref = "secret://file/hedge.toml"
instruments = [{ native_symbol = "BTCUSDT", asset_id = 1002, price_tick = { coefficient = 1, scale = 1 }, quantity_lot = { coefficient = 1, scale = 3 }, contract_multiplier = { coefficient = 1, scale = 0 } }]
currencies = [{ native_currency = "USDT", currency_id = 1, amount_unit = { coefficient = 1, scale = 8 } }]
ownership = "ObserveAll"
shutdown_order_policy = "CancelAll"
enabled = true
definition_version = 1

[[strategies]]
strategy_key = "cross_exchange_mm"
strategy_id = 9101
package = { loader_type = "native", uri = "file://artifacts/strategy.titan", expected_digest = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1] }
markets = [
  { local_market_no = 0, local_asset_no = 0, source_key = "hedge", asset_id = 1002, data_mode = "tick" },
  { local_market_no = 1, local_asset_no = 1, source_key = "maker", asset_id = 1001, data_mode = "tick" },
]
accounts = [
  { local_account_no = 0, account_key = "maker", tradable_assets = [{ local_asset_no = 1, asset_id = 1001 }] },
  { local_account_no = 1, account_key = "hedge", tradable_assets = [{ local_asset_no = 0, asset_id = 1002 }] },
]
risk_scope = "test"
runtime = { async_lane_capacity = 8192, critical_reserve = 2048, reliable_pending_capacity = 1024, worker_policy = "spin_sleep", timer_capacity = 64, %(timer)s max_handler_duration = { secs = 0, nanos = 100000000 }, startup_timeout = { secs = 30, nanos = 0 }, stop_timeout = { secs = 10, nanos = 0 } }
enabled = true
"""

HEDGE_LOT = "quantity_lot = { coefficient = 1, scale = 3 }, contract_multiplier = { coefficient = 1, scale = 0 } }"
HEDGE_MULTIPLIER = "quantity_lot = { coefficient = 1, scale = 3 }, contract_multiplier = { coefficient = 2, scale = 0 } }"


def config():
    return BASE % {"timer": TIMER}


VALID_REPORT = {
    "valid": True,
    "instruments": [{"source_key": "maker", "native_symbol": "BTC-USDT-SWAP", "asset_id": 1001,
                     "base_delta_per_lot": "0.01"}],
    "errors": [],
}


def failed_report(code, subject="unit-test", detail="unit check failed"):
    return {"valid": False, "instruments": [], "errors": [
        {"code": code, "subject": subject, "detail": detail}
    ]}


class PreflightTests(unittest.TestCase):
    def run_preflight(self, text, *, allow_shadow=False, check_secrets=False,
                      report=VALID_REPORT, titan=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "runtime.toml"
        path.write_text(text)
        args = type("Args", (), {
            "maker_market": 1, "hedge_market": 0, "allow_shadow": allow_shadow,
            "check_secrets": check_secrets, "titan": titan,
        })()
        validator = None if report is None else (lambda _path: report)
        return path, preflight.preflight(path, args, validator=validator)

    @staticmethod
    def levels(report):
        return {item["check"]: item["level"] for item in report.items}

    def failures(self, report):
        return [item for item in report.items if item["level"] == preflight.FAIL]

    def test_exact_integer_ratio_and_shared_collateral_are_accepted(self):
        _, report = self.run_preflight(config())
        levels = self.levels(report)
        self.assertEqual(levels["platform-units"], preflight.PASS)
        self.assertEqual(levels["collateral"], preflight.PASS)
        self.assertEqual(levels["timer"], preflight.PASS)
        # Controlled live still has no two-leg execution envelope: shadow is required.
        self.assertEqual(levels["execution-limits"], preflight.FAIL)
        _, shadow = self.run_preflight(config(), allow_shadow=True)
        self.assertEqual(self.failures(shadow), [])

    def test_missing_timer_is_rejected(self):
        _, report = self.run_preflight(config().replace(TIMER, ""), allow_shadow=True)
        self.assertEqual(self.levels(report)["timer"], preflight.FAIL)

    def test_non_integer_lot_ratio_is_rejected(self):
        # The platform validator owns the arithmetic; the preflight must surface its reason code.
        _, report = self.run_preflight(
            config(),
            allow_shadow=True,
            report=failed_report("instrument_lot_ratio_not_integral"),
        )
        self.assertEqual(self.levels(report)["platform-units"], preflight.FAIL)

    def test_cross_currency_pair_is_rejected(self):
        text = config().replace(
            'native_currency = "USDT", currency_id = 1',
            'native_currency = "USDC", currency_id = 2',
            1,
        )
        _, report = self.run_preflight(text, allow_shadow=True)
        self.assertEqual(self.levels(report)["collateral"], preflight.FAIL)

    def test_instrument_unit_mismatch_is_rejected(self):
        text = config().replace(HEDGE_LOT, HEDGE_MULTIPLIER, 1)
        _, report = self.run_preflight(
            text,
            allow_shadow=True,
            report=failed_report("instrument_units_market_account_mismatch"),
        )
        self.assertEqual(self.levels(report)["platform-units"], preflight.FAIL)

    def test_missing_platform_validator_requires_shadow(self):
        _, report = self.run_preflight(config(), report=None)
        self.assertEqual(self.levels(report)["platform-units"], preflight.FAIL)
        _, shadow = self.run_preflight(config(), allow_shadow=True, report=None)
        self.assertEqual(self.levels(shadow)["platform-units"], preflight.WARN)

    def test_secret_permissions_are_enforced(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "secrets").mkdir()
        path = root / "runtime.toml"
        path.write_text(config())
        for name in ("maker.toml", "hedge.toml"):
            target = root / "secrets" / name
            target.write_text('api_key = ""\n')
            target.chmod(0o600)
        args = type("Args", (), {"maker_market": 1, "hedge_market": 0,
                                 "allow_shadow": True, "check_secrets": True, "titan": None})()
        validator = lambda _path: VALID_REPORT
        report = preflight.preflight(path, args, validator=validator)
        self.assertEqual(self.failures(report), [])
        (root / "secrets" / "maker.toml").chmod(0o644)
        report = preflight.preflight(path, args, validator=validator)
        self.assertTrue(any(
            item["level"] == preflight.FAIL and item["check"] == "secrets"
            for item in report.items
        ))


class TraceSchema4EnvelopeTest(unittest.TestCase):
    """The verification scripts hand-build trace schema 4 inputs; keep them self-consistent.

    Both scripts inline the trace they replay. If their metadata digest or ABI identity drifts from
    what the host recomputes, the scripts fail for the wrong reason (an input bug that reads like a
    product regression). These checks pin the parts that can be recomputed offline.
    """

    def test_metadata_digest_helpers_match_the_checked_in_fixture(self):
        import hashlib
        import importlib.util
        import json
        import textwrap

        root = Path(__file__).resolve().parent.parent
        metadata = json.loads((root / "fixtures" / "grid_v13_metadata.json").read_text())["metadata"]
        for name, function, following in (
            ("verify_cli_bundle", "canonical_digest", "trace_schema_4"),
            ("verify_depth_trace", "canonical_snapshot_digest", "schema_4_envelope"),
        ):
            source = (root / "scripts" / f"{name}.py").read_text()
            start = source.index(f"def {function}")
            end = source.index(f"def {following}")
            namespace = {"json": json, "hashlib": hashlib}
            exec(textwrap.dedent(source[start:end]), namespace)  # noqa: S102 - local test helper
            self.assertEqual(
                namespace[function](metadata),
                metadata["digest"],
                f"{name}.{function} does not reproduce the canonical snapshot digest",
            )

    def test_envelopes_carry_schema_4_identity_and_accounts(self):
        import importlib.util
        import json

        root = Path(__file__).resolve().parent.parent
        spec = importlib.util.spec_from_file_location(
            "verify_depth_trace", root / "scripts" / "verify_depth_trace.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        envelope = module.schema_4_envelope(root)
        self.assertEqual(envelope["schema_version"], 4)
        self.assertEqual(envelope["runtime_abi"]["abi_version"], 14)
        self.assertTrue(envelope["accounts"], "schema 4 object strategies need account definitions")
        self.assertTrue(envelope["account_instruments"])
        self.assertEqual(
            envelope["metadata"]["digest"],
            module.canonical_snapshot_digest(envelope["metadata"]),
        )
        fixture = json.loads((root / "fixtures" / "grid_v13_metadata.json").read_text())
        self.assertEqual(envelope["runtime_abi"], fixture["runtime_abi"])


if __name__ == "__main__":
    unittest.main()
