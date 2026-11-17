"""Matrix item 4: strategies must not carry exchange facts as parameters."""

import subprocess
import tempfile
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[3]
GUARD = ROOT / "scripts" / "check_strategy_no_exchange_facts.py"


class TestStrategyExchangeFacts(unittest.TestCase):
    def test_repository_strategies_declare_no_exchange_fact_parameters(self):
        result = subprocess.run(
            [sys.executable, str(GUARD)],
            capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("no exchange facts as parameters", result.stdout)

    def test_guard_rejects_a_parameter_list_with_exchange_facts(self):
        # A minimal synthetic strategy: the guard is name-based and needs no real module.
        offender = (
            "from titan_strategy.parameters import FloatParam\n"
            "spec = dict(\n"
            "    parameters=(FloatParam(\"maker_fee_bps\", required=False, default=0.0),),\n"
            "    subscriptions=(),\n"
            ")\n"
        )
        temporary = Path(self.enterContext(tempfile.TemporaryDirectory())) / "probe.py"
        try:
            temporary.write_text(offender, encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(GUARD), str(temporary)],
                capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("maker_fee_bps", result.stderr)
        finally:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
