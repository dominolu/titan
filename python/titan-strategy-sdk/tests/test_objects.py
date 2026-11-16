"""Object accessors: native Numba code reading a handcrafted object table."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import numpy as np
from llvmlite import ir
from numba import from_dtype, njit
from numba.core import types
from numba.extending import intrinsic

from titan_strategy import abi, context  # noqa: F401 - registers overloads
from titan_strategy.context import StrategyContextType
from titan_strategy.definition import Capability
from titan_strategy.static_compiler import (
    StrategyCompileError, describe_strategy, load_strategy_definition,
    validate_strategy_definition,
)

STATE_DTYPE = np.dtype([("counter", "<i8")], align=True)
CONTEXT_TYPE = StrategyContextType(STATE_DTYPE)


@intrinsic
def _as_context(typingctx, pointer):
    if not isinstance(pointer, types.Integer):
        return None
    signature = CONTEXT_TYPE(pointer)

    def codegen(context, builder, sig, args):
        del context, sig
        raw = args[0]
        target = ir.IntType(64)
        if raw.type.width < 64:
            raw = builder.zext(raw, target)
        elif raw.type.width > 64:
            raw = builder.trunc(raw, target)
        return builder.bitcast(
            builder.inttoptr(raw, ir.IntType(8).as_pointer()), ir.IntType(8).as_pointer()
        )

    return signature, codegen


@njit
def symbol_probe(address):
    context = _as_context(address)
    symbol = context.symbol(0)
    return (
        np.int64(symbol["asset_id"]),
        np.int64(symbol["price_tick"]["coefficient"]),
        np.int64(symbol["price_tick"]["scale"]),
        np.int64(symbol["base_delta_per_lot"]["coefficient"]),
        np.int64(symbol["base_delta_per_lot"]["scale"]),
        np.int64(symbol["min_qty_lots"]),
        np.int64(symbol["min_notional_tick_lots"]),
        np.int64(symbol["quote_currency_no"]),
        np.int64(symbol["flags"]),
    )


@njit
def account_probe(address):
    context = _as_context(address)
    account = context.account_info(0)
    currency = context.currency(0, 1)
    missing_currency = context.currency(0, 9)
    return (
        np.int64(account["account_id"]),
        np.int64(account["quote_currency_no"]),
        np.int64(account["currency_count"]),
        np.int64(currency["amount_unit"]["coefficient"]),
        np.int64(currency["amount_unit"]["scale"]),
        np.int64(missing_currency["currency_no"]),
    )


@njit
def pair_probe(address):
    context = _as_context(address)
    pair = context.pair(0)
    return (
        np.int64(pair["hedge_lots_per_maker_lot"]),
        np.int64(pair["hedge_ticks_per_maker_tick_num"]),
        np.int64(pair["hedge_ticks_per_maker_tick_den"]),
        np.int64(pair["min_hedge_lots"]),
        np.int64(pair["min_hedge_notional_quote"]["coefficient"]),
        np.int64(pair["flags"]),
    )


@njit
def status_fee_probe(address):
    context = _as_context(address)
    status = context.status()
    valid_fee = context.fee(1)
    missing_fee = context.fee(0)
    return (
        np.int64(status["lifecycle"]),
        np.int64(valid_fee["flags"]),
        np.int64(valid_fee["maker_fee_bps"]),
        np.int64(missing_fee["flags"]),
        np.int64(missing_fee["maker_fee_bps"]),
    )


@njit
def missing_symbol_probe(address):
    context = _as_context(address)
    symbol = context.symbol(9)
    return np.int64(symbol["asset_id"])


class Table:
    """Handcrafted object table kept alive for the duration of one probe call."""

    def __init__(self):
        self.buffers = []
        symbol = np.zeros(1, dtype=abi.symbol_payload_dtype)
        symbol[0]["local_asset_no"] = 0
        symbol[0]["asset_id"] = 4101
        symbol[0]["quote_currency_no"] = 1
        symbol[0]["margin_currency_no"] = 2
        symbol[0]["flags"] = 1
        symbol[0]["price_tick"]["coefficient"] = 1
        symbol[0]["price_tick"]["scale"] = 1
        symbol[0]["quantity_lot"]["coefficient"] = 1
        symbol[0]["quantity_lot"]["scale"] = 2
        symbol[0]["contract_multiplier"]["coefficient"] = 1
        symbol[0]["contract_multiplier"]["scale"] = 0
        symbol[0]["base_delta_per_lot"]["coefficient"] = 1
        symbol[0]["base_delta_per_lot"]["scale"] = 2
        symbol[0]["min_quantity"]["coefficient"] = 1
        symbol[0]["min_quantity"]["scale"] = 2
        symbol[0]["min_notional_quote"]["coefficient"] = 5
        symbol[0]["min_notional_quote"]["scale"] = 0
        symbol[0]["min_qty_lots"] = 1
        symbol[0]["min_notional_tick_lots"] = 5000
        account = np.zeros(1, dtype=abi.account_payload_dtype)
        account[0]["local_account_no"] = 0
        account[0]["account_id"] = 77
        account[0]["quote_currency_no"] = 1
        account[0]["margin_currency_no"] = 2
        account[0]["currency_start"] = 0
        account[0]["currency_count"] = 1
        account[0]["flags"] = 1
        currency = np.zeros(1, dtype=abi.currency_payload_dtype)
        currency[0]["local_account_no"] = 0
        currency[0]["currency_no"] = 1
        currency[0]["flags"] = 1
        currency[0]["amount_unit"]["coefficient"] = 1
        currency[0]["amount_unit"]["scale"] = 6
        pair = np.zeros(1, dtype=abi.pair_payload_dtype)
        pair[0]["maker_local_asset_no"] = 0
        pair[0]["hedge_local_asset_no"] = 1
        pair[0]["hedge_lots_per_maker_lot"] = 10
        pair[0]["hedge_ticks_per_maker_tick_num"] = 1
        pair[0]["hedge_ticks_per_maker_tick_den"] = 2
        pair[0]["min_hedge_lots"] = 10
        pair[0]["min_hedge_notional_quote"]["coefficient"] = 5
        pair[0]["min_hedge_notional_quote"]["scale"] = 0
        pair[0]["flags"] = 3
        status = np.zeros(1, dtype=abi.status_payload_dtype)
        status[0]["lifecycle"] = 3
        fee_valid = np.zeros(1, dtype=abi.fee_payload_dtype)
        fee_valid[0]["local_account_no"] = 1
        fee_valid[0]["flags"] = 1
        fee_valid[0]["maker_fee_bps"] = -2
        fee_invalid = np.zeros(1, dtype=abi.fee_payload_dtype)
        fee_invalid[0]["local_account_no"] = 0
        fee_invalid[0]["flags"] = 0
        self.buffers = [symbol, account, currency, pair, status, fee_valid, fee_invalid]
        refs = np.zeros(7, dtype=abi.object_ref_dtype)
        entries = (
            (abi.DOMAIN_SYMBOL, 0, 1, symbol),
            (abi.DOMAIN_ACCOUNT, 0, 1, account),
            (abi.DOMAIN_CURRENCY, 0, 1, currency),
            (abi.DOMAIN_PAIR, 0, 1, pair),
            (abi.DOMAIN_STATUS, 0, 1 | 2, status),
            (abi.DOMAIN_FEE, 1, 1, fee_valid),
            (abi.DOMAIN_FEE, 0, 0, fee_invalid),
        )
        for index, (domain, object_id, flags, buffer) in enumerate(entries):
            refs[index]["domain"] = domain
            refs[index]["object_id"] = object_id
            refs[index]["flags"] = flags
            refs[index]["schema_major"] = 1
            refs[index]["schema_minor"] = 1
            refs[index]["payload"] = buffer.ctypes.data
            refs[index]["payload_len"] = buffer.nbytes
        self.refs = refs
        self.context = np.zeros(1, dtype=abi.runtime_context_dtype)
        self.context[0]["struct_size"] = abi.runtime_context_dtype.itemsize
        self.context[0]["abi_version"] = abi.ABI_VERSION
        self.context[0]["objects_ptr"] = refs.ctypes.data
        self.context[0]["objects_len"] = refs.nbytes
        self.context[0]["object_schema_minor"] = 1
        self.context[0]["last_error_code"] = 0
        self.buffers.append(refs)

    @property
    def address(self):
        return self.context.ctypes.data

    def last_error(self):
        return int(self.context[0]["last_error_code"])


class TestObjectAccessors(unittest.TestCase):
    def test_symbol_and_account_accessors_return_authoritative_values(self):
        table = Table()
        self.assertEqual(
            symbol_probe(table.address),
            (4101, 1, 1, 1, 2, 1, 5000, 1, 1),
        )
        self.assertEqual(table.last_error(), 0)
        self.assertEqual(
            account_probe(table.address),
            (77, 1, 1, 1, 6, 0),
        )
        # A missing currency sets the lifecycle error but does not corrupt the found one.
        self.assertEqual(table.last_error(), -2)

    def test_pair_status_and_fee_objects(self):
        table = Table()
        self.assertEqual(pair_probe(table.address), (10, 1, 2, 10, 5, 3))
        self.assertEqual(status_fee_probe(table.address), (3, 1, -2, 0, 0))
        self.assertEqual(table.last_error(), 0, "an absent fee is not an error")

    def test_missing_required_object_sets_the_error_code(self):
        table = Table()
        self.assertEqual(missing_symbol_probe(table.address), 0)
        self.assertEqual(table.last_error(), -2)

    def test_newer_payload_minor_is_read_through_the_pinned_prefix(self):
        table = Table()
        table.refs[0]["schema_minor"] = 2
        self.assertEqual(symbol_probe(table.address)[0], 4101)
        self.assertEqual(table.last_error(), 0)

    def test_null_or_misaligned_payload_pointer_is_refused(self):
        table = Table()
        symbol_address = int(table.refs[0]["payload"])
        for bad in (0, symbol_address + 1):
            table.refs[0]["payload"] = bad
            table.context[0]["last_error_code"] = 0
            self.assertEqual(symbol_probe(table.address)[0], 0)
            self.assertEqual(table.last_error(), -2, f"pointer {bad:#x} must be refused")
        table.refs[0]["payload"] = symbol_address
        self.assertEqual(symbol_probe(table.address)[0], 4101)

    def test_reference_version_gate_rejects_future_payloads(self):
        table = Table()
        table.refs[0]["schema_minor"] = 0
        self.assertEqual(missing_symbol_probe(table.address), 0)
        self.assertEqual(table.last_error(), -2)

    def test_payload_length_is_checked(self):
        table = Table()
        table.refs[0]["payload_len"] = 8
        self.assertEqual(symbol_probe(table.address)[0], 0)
        self.assertEqual(table.last_error(), 0, "a short payload yields a zero record without error")


class TestStaticContract(unittest.TestCase):
    source = '''
from numba import njit
from titan_strategy.definition import Capability, EventSubscription, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.types import EventKind, EventQos

state_dtype = record(counter=int64)

SPEC = StrategySpec(
    strategy_id="object_probe",
    strategy_version="1.0.0",
    state_schema_version=1,
    subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
    capabilities=Capability.MARKET_DATA | Capability.OBJECT_CONTEXT,
)

@njit
def on_tick(ctx):
    ctx.state["counter"] = np.int64(ctx.symbol(0)["min_qty_lots"])

def build(parameters):
    return StrategyDefinition(SPEC, new_state(state_dtype), {"on_tick": on_tick}, {})
'''

    def compile_source(self, source):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "strategy.py"
        path.write_text("import numpy as np\n" + source)
        runtime_abi = {"abi_version": abi.ABI_VERSION, "fingerprint": abi.ABI_FINGERPRINT.hex()}
        return validate_strategy_definition(load_strategy_definition(path, {}), runtime_abi)

    def test_object_accessor_requires_the_capability(self):
        source = self.source.replace(
            "capabilities=Capability.MARKET_DATA | Capability.OBJECT_CONTEXT,",
            "capabilities=Capability.MARKET_DATA,",
        )
        with self.assertRaisesRegex(StrategyCompileError, "OBJECT_CONTEXT"):
            self.compile_source(source)

    def test_declared_capability_is_accepted(self):
        strategy = self.compile_source(self.source)
        self.assertTrue(strategy.definition.spec.capabilities & Capability.OBJECT_CONTEXT)

    def test_descriptor_reports_object_requirements(self):
        strategy = self.compile_source(self.source)
        runtime_abi = {
            "abi_version": abi.ABI_VERSION,
            "fingerprint": abi.ABI_FINGERPRINT.hex(),
            "object_schema_hash": abi.OBJECT_SCHEMA_HASH.hex(),
        }
        described = describe_strategy(strategy, runtime_abi)
        self.assertEqual(described["abi_version"], abi.ABI_VERSION)
        self.assertEqual(described["abi_fingerprint"], list(abi.ABI_FINGERPRINT))
        self.assertEqual(described["object_schema_major"], 1)
        self.assertEqual(described["object_schema_minor"], 1)
        self.assertEqual(described["object_schema_hash"], list(abi.OBJECT_SCHEMA_HASH))
        self.assertFalse(described["requires_fee"])
        domains = [item["domain"] for item in described["object_requirements"]]
        self.assertEqual(domains, [abi.DOMAIN_SYMBOL, abi.DOMAIN_ACCOUNT,
                                   abi.DOMAIN_CURRENCY])
        symbol = described["object_requirements"][0]
        self.assertEqual(symbol["min_payload_len"], abi.symbol_payload_dtype.itemsize)
        self.assertEqual(symbol["required_fields"],
                         (1 << 0) | (1 << 1) | (1 << 2) | (1 << 3)
                         | (1 << 8) | (1 << 9) | (1 << 10) | (1 << 11))
        # There is one descriptor shape: object keys are always present, whether or not the
        # caller supplied a runtime ABI.
        default_described = describe_strategy(strategy)
        self.assertEqual(default_described["abi_version"], abi.ABI_VERSION)
        self.assertIn("object_schema_hash", default_described)
        self.assertIn("object_requirements", default_described)


class TestCompiledObjectReader(unittest.TestCase):
    def test_compiled_callback_reads_the_authoritative_object_table(self):
        root = Path(__file__).parents[3]
        binary = os.environ.get("TITAN_TEST_BINARY", str(root / "target/debug/titan"))
        source = Path(__file__).parent / "fixtures" / "object_reader.py"
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "object_reader"
            result = subprocess.run(
                [binary, "strategy", "compile", "--strategy", str(source),
                 "--output", str(output), "--json"],
                capture_output=True, text=True, timeout=600,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["metadata"]["abi_version"], abi.ABI_VERSION)
            self.assertGreaterEqual(report["tests"]["declared"], 1)
            self.assertEqual(report["tests"]["passed"], report["tests"]["declared"])


if __name__ == "__main__":
    unittest.main()
