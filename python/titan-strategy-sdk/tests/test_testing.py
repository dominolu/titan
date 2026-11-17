"""Object-table fixtures for the declarative TESTS harness."""

import unittest
from pathlib import Path

import numpy as np

from titan_strategy import abi, testing


def symbol(asset_no=0, *, price_tick=0.1, min_quantity=0.01):
    return dict(
        local_asset_no=asset_no,
        asset_id=asset_no + 1,
        quote_currency_no=1,
        margin_currency_no=1,
        flags=1,
        price_tick={"coefficient": 1, "scale": 1},
        quantity_lot={"coefficient": 1, "scale": 2},
        contract_multiplier={"coefficient": 1, "scale": 0},
        base_delta_per_lot={"coefficient": 1, "scale": 2},
        min_quantity={"coefficient": 1, "scale": 2},
        min_notional_quote={"coefficient": 5, "scale": 0},
        min_qty_lots=1,
        min_notional_tick_lots=5000,
    )


def account(account_no=0):
    return dict(
        local_account_no=account_no,
        account_id=account_no + 10,
        quote_currency_no=1,
        margin_currency_no=1,
        currency_start=0,
        currency_count=1,
        flags=1,
    )


class TestObjectFixtures(unittest.TestCase):
    def test_objects_description_is_json_compatible(self):
        description = testing.objects(
            symbols=[symbol(0)],
            accounts=[account(0)],
            currencies=[dict(local_account_no=0, currency_no=1, flags=1,
                             amount_unit={"coefficient": 1, "scale": 6})],
            pairs=[dict(maker_local_asset_no=0, hedge_local_asset_no=1,
                        hedge_lots_per_maker_lot=10, min_hedge_lots=10,
                        min_hedge_notional_quote={"coefficient": 5, "scale": 0})],
            fees=[dict(local_account_no=0, flags=1, maker_fee_bps=-2, taker_fee_bps=5)],
            status={"lifecycle": 2},
        )
        self.assertEqual(description["status"], {"lifecycle": 2})
        self.assertEqual(description["symbols"][0]["asset_id"], 1)
        with self.assertRaises(ValueError):
            testing.objects(symbols=[symbol(i) for i in range(65)])

    def test_build_orders_refs_and_keeps_payloads_addressable(self):
        description = testing.objects(
            symbols=[symbol(1), symbol(0)],
            accounts=[account(0)],
            currencies=[dict(local_account_no=0, currency_no=1, flags=1,
                             amount_unit={"coefficient": 1, "scale": 6})],
            pairs=[dict(maker_local_asset_no=1, hedge_local_asset_no=0,
                        hedge_lots_per_maker_lot=10, min_hedge_lots=10,
                        min_hedge_notional_quote={"coefficient": 5, "scale": 0})],
            fees=[
                dict(local_account_no=0, flags=0, maker_fee_bps=0, taker_fee_bps=0),
                dict(local_account_no=1, flags=1, maker_fee_bps=-2, taker_fee_bps=5),
            ],
            status={"lifecycle": 3},
        )
        refs, buffers, status = testing._build_object_table(description)
        self.assertEqual(refs.dtype.itemsize, 32)
        expected = [
            (abi.DOMAIN_SYMBOL, 0, 1),
            (abi.DOMAIN_SYMBOL, 1, 1),
            (abi.DOMAIN_ACCOUNT, 0, 1),
            (abi.DOMAIN_CURRENCY, 0, 1),
            (abi.DOMAIN_PAIR, 0, 1),
            (abi.DOMAIN_STATUS, 0, 1 | 2),
            (abi.DOMAIN_FEE, 0, 0),
            (abi.DOMAIN_FEE, 1, 1),
        ]
        self.assertEqual(
            [(int(ref["domain"]), int(ref["object_id"]), int(ref["flags"])) for ref in refs],
            expected,
        )
        for ref in refs:
            self.assertEqual(int(ref["schema_major"]), abi.PAYLOAD_MAJOR)
            self.assertEqual(int(ref["schema_minor"]), abi.PAYLOAD_MINOR)
            self.assertNotEqual(int(ref["payload"]), 0)
            self.assertGreater(int(ref["payload_len"]), 0)
        self.assertEqual(int(status[0]["lifecycle"]), 3)
        # Payload rows keep the declaration order; only the reference array is sorted.
        self.assertEqual(int(buffers["symbols"][0]["asset_id"]), 2)
        # Fees are addressed by account number and may be invalid.
        self.assertEqual(int(buffers["fees"][0]["flags"]), 0)

    def test_validation_accepts_objects_and_object_status(self):
        cases = testing.validate_tests([{
            "name": "objects",
            "objects": testing.objects(symbols=[symbol()], accounts=[account()]),
            "steps": [{
                "callback": "on_start",
                "object_status": {"lifecycle": 3},
                "expect": {"commands.submit_count": 0},
            }],
        }])
        self.assertEqual(cases[0]["objects"]["symbols"][0]["asset_id"], 1)
        with self.assertRaises(ValueError):
            testing.validate_tests([{"name": "bad", "objects": {"symbols": []},
                                     "steps": [{"callback": "on_start", "unknown_key": 1}]}])

    def test_objects_are_accepted_by_the_single_abi_harness(self):
        # One ABI means the harness always understands objects; the failure a scene hits first is
        # the missing shared library, not an ABI objection.
        scene = testing.validate_tests([{
            "name": "objects",
            "objects": testing.objects(symbols=[symbol()], accounts=[account()]),
            "steps": [{"callback": "on_start", "expect": {"commands.submit_count": 0}}],
        }])
        with self.assertRaises(OSError):
            testing.run_native_tests(object(), Path("unused.so"), scene)

    def test_duplicate_fee_objects_are_rejected(self):
        description = testing.objects(
            symbols=[symbol()],
            accounts=[account()],
            fees=[dict(local_account_no=0, flags=1), dict(local_account_no=0, flags=0)],
        )
        with self.assertRaisesRegex(ValueError, "duplicate fee"):
            testing._build_object_table(description)


if __name__ == "__main__":
    unittest.main()
