import hashlib
import unittest

import numpy as np

from titan_strategy import abi_v13
from titan_strategy.definition import Capability, EventSubscription, StrategySpec
from titan_strategy.parameters import EnumParam, FloatParam, IntParam, ParameterError
from titan_strategy.state import (
    StateSchemaError, array, canonical_state_schema, float64, int32, int64, new_state, record,
    state_schema_hash, validate_state_dtype,
)
from titan_strategy.types import EventKind, EventQos


class TestV13State(unittest.TestCase):
    def test_nested_record_and_fixed_array_have_stable_schema(self):
        dtype = record(pair=record(status=int32, ratio=float64), window=array(int64, 4))
        layout = validate_state_dtype(dtype, max_state_bytes=1024, max_alignment=8)
        schema = canonical_state_schema(layout, schema_version=3)
        self.assertEqual(state_schema_hash(schema), hashlib.sha256(schema).digest())
        self.assertEqual(layout.alignment, 8)
        state = new_state(dtype)
        state[0]["pair"]["status"] = 7
        state[0]["window"][2] = 11
        self.assertEqual(int(state[0]["window"][2]), 11)

    def test_invalid_state_types_are_rejected(self):
        with self.assertRaises(StateSchemaError):
            record(value=np.dtype(object))
        with self.assertRaises(StateSchemaError):
            array(int64, 0)
        with self.assertRaises(StateSchemaError):
            validate_state_dtype(np.dtype("<i8"), max_state_bytes=8, max_alignment=8)


class TestV13Definition(unittest.TestCase):
    def test_parameters_schema_and_qos_contract(self):
        spec = StrategySpec(
            strategy_id="sample",
            strategy_version="1.2.3",
            state_schema_version=1,
            parameters=(
                FloatParam("ratio", minimum=0, exclusive_minimum=True),
                IntParam("limit", required=False, default=4, minimum=1),
                EnumParam("mode", values=("A", "B")),
            ),
            subscriptions=(EventSubscription(EventKind.BBO, "on_tick", 1, EventQos.LATEST),),
            capabilities=Capability.MARKET_DATA,
        )
        values = spec.validate_parameters({"ratio": 1.5, "mode": "A"})
        self.assertEqual(values, {"ratio": 1.5, "limit": 4, "mode": "A"})
        self.assertFalse(spec.parameter_schema()["additionalProperties"])
        with self.assertRaises(ParameterError):
            spec.validate_parameters({"ratio": 1.5, "mode": "C"})
        with self.assertRaises(ValueError):
            EventSubscription(EventKind.FILL, "on_fill", 1, EventQos.BEST_EFFORT)


class TestV13Abi(unittest.TestCase):
    def test_context_and_descriptor_layout(self):
        self.assertEqual(abi_v13.runtime_context_dtype.itemsize, 392)
        self.assertEqual(abi_v13.runtime_context_dtype.alignment, 8)
        self.assertEqual(abi_v13.native_descriptor_dtype.itemsize, 96)
        self.assertEqual(len(abi_v13.ABI_FINGERPRINT), 32)
        self.assertEqual(
            abi_v13.callback_mask(("on_start", "on_tick", "on_fill", "on_order", "on_cancel", "on_stop")),
            2163,
        )


if __name__ == "__main__":
    unittest.main()
