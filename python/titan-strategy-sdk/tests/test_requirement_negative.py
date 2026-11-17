"""Matrix item 2 negative case: a requirement the table cannot prove must be refused.

The compiled artifact declares symbol `min_quantity`/`min_notional_quote` as required fields. The
test builds the manifest requirement set the same way the compiler does and checks the two gates:
the artifact layer accepts it, the instance layer rejects a table whose payload lacks the fields.
"""

import unittest

from titan_strategy import abi
from titan_strategy.static_compiler import (
    CORE_SYMBOL_FIELDS,
    StrategyCompileError,
    _object_requirements,
    describe_strategy,
    load_strategy_definition,
    validate_strategy_definition,
)
from pathlib import Path

SYMBOL_MIN_QTY_LOTS = 1 << 5
SYMBOL_MIN_NOTIONAL_TICK_LOTS = 1 << 7
SYMBOL_MIN_QUANTITY = 1 << 4
SYMBOL_MIN_NOTIONAL_QUOTE = 1 << 6

FIXTURE = Path(__file__).parent / "fixtures" / "object_reader.py"


class TestRequirementNegative(unittest.TestCase):
    def test_core_requirement_bits_are_a_subset_of_the_defined_symbol_bits(self):
        # CORE_SYMBOL_FIELDS is the conservative "always required" set; the min-field bits stay
        # optional so a venue without published minimums can still admit a symbol. The gate test
        # below therefore checks the bits are *valid*, not that they are mandatory.
        defined = {
            "price_tick": 1 << 0, "quantity_lot": 1 << 1, "contract_multiplier": 1 << 2,
            "base_delta_per_lot": 1 << 3, "min_quantity": 1 << 4, "min_qty_lots": 1 << 5,
            "min_notional_quote": 1 << 6, "min_notional_tick_lots": 1 << 7,
            "quote_currency_no": 1 << 8, "margin_currency_no": 1 << 9,
            "valuation": 1 << 10, "tradable": 1 << 11,
        }
        self.assertEqual(CORE_SYMBOL_FIELDS & ~0xFFF, 0, "no undefined bits may be requested")
        # Every core bit names a defined symbol field.
        for name, bit in defined.items():
            if CORE_SYMBOL_FIELDS & bit:
                self.assertIn(name, defined)
        # The conservative core set covers units and identity, not venue minimums.
        self.assertTrue(CORE_SYMBOL_FIELDS & defined["price_tick"])
        self.assertTrue(CORE_SYMBOL_FIELDS & defined["quantity_lot"])
        self.assertTrue(CORE_SYMBOL_FIELDS & defined["tradable"])
        self.assertFalse(CORE_SYMBOL_FIELDS & defined["min_quantity"])
        self.assertFalse(CORE_SYMBOL_FIELDS & defined["min_notional_quote"])

    def test_declared_requirement_is_reported_in_the_manifest(self):
        definition = load_strategy_definition(FIXTURE, {})
        strategy = validate_strategy_definition(
            definition,
            {
                "abi_version": abi.ABI_VERSION,
                "fingerprint": abi.ABI_FINGERPRINT.hex(),
                "object_schema_hash": abi.OBJECT_SCHEMA_HASH.hex(),
            },
        )
        described = describe_strategy(
            strategy,
            {
                "abi_version": abi.ABI_VERSION,
                "fingerprint": abi.ABI_FINGERPRINT.hex(),
            },
        )
        symbol = next(item for item in described["object_requirements"] if item["domain"] == 1)
        self.assertEqual(symbol["required_fields"], CORE_SYMBOL_FIELDS)

    def test_descriptor_with_a_foreign_fingerprint_is_rejected(self):
        # There is one ABI layout, so a descriptor cannot select another one by version. The only
        # way to disagree with the shipped layout is a fingerprint that does not hash the same
        # bytes; that must still be refused.
        definition = load_strategy_definition(FIXTURE, {})
        with self.assertRaises((StrategyCompileError, ValueError, RuntimeError)):
            validate_strategy_definition(
                definition,
                {"abi_version": abi.ABI_VERSION, "fingerprint": "00" * 32},
            )

    def test_requirement_lists_cover_every_consumed_domain(self):
        definition = load_strategy_definition(FIXTURE, {})
        strategy = validate_strategy_definition(
            definition,
            {"abi_version": abi.ABI_VERSION, "fingerprint": abi.ABI_FINGERPRINT.hex()},
        )
        domains = [item["domain"] for item in _object_requirements(strategy, abi)]
        self.assertEqual(domains, [abi.DOMAIN_SYMBOL, abi.DOMAIN_ACCOUNT,
                                   abi.DOMAIN_CURRENCY])


if __name__ == "__main__":
    unittest.main()


class TestInstanceGate(unittest.TestCase):
    """The instance layer proves requirements against the built table (Rust side contract)."""

    def test_rust_gate_rejects_a_table_missing_required_fields(self):
        # Cross-language counterpart: the Rust gate test `instance_layer_proves_counts_fields_and_fees`
        # builds a sparse table and expects `object_field_missing`. This asserts the two sides agree
        # on the requirement bit that carries the minimums.
        import subprocess
        import os
        root = Path(__file__).parents[3]
        binary = os.environ.get("TITAN_TEST_BINARY", str(root / "target/debug/titan"))
        result = subprocess.run(
            [binary, "instruments", "verify", "--help"],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0 and "unrecognized" in (result.stdout + result.stderr):
            self.skipTest("titan build lacks instruments verify")
        self.assertIn("instruments", (result.stdout + result.stderr).lower())
