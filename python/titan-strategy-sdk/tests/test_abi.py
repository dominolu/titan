"""Object-context layout and cross-language schema fixtures."""

import json
import unittest
from pathlib import Path

import numpy as np

from titan_strategy import abi


class TestAbi(unittest.TestCase):
    def test_root_and_object_ref_layout(self):
        self.assertEqual(abi.runtime_context_dtype.itemsize, 440)
        self.assertEqual(abi.runtime_context_dtype.alignment, 8)
        self.assertEqual(abi.object_ref_dtype.itemsize, 32)
        self.assertEqual(abi.object_ref_dtype.alignment, 8)
        fields = abi.runtime_context_dtype.fields
        self.assertEqual(int(fields["objects_ptr"][1]), 392)
        self.assertEqual(int(fields["objects_len"][1]), 400)
        self.assertEqual(int(fields["object_schema_minor"][1]), 408)
        self.assertEqual(int(fields["object_query"][1]), 416)
        self.assertEqual(int(fields["reserved_ext"][1]), 432)
        ref = abi.object_ref_dtype.fields
        self.assertEqual(int(ref["payload"][1]), 16)
        self.assertEqual(int(ref["payload_len"][1]), 24)

    def test_payload_sizes_and_domain_order(self):
        sizes = [int(payload[2].itemsize) for payload in abi.OBJECT_PAYLOADS]
        self.assertEqual(sizes, [152, 48, 32, 80, 40, 32])
        domains = [payload[0] for payload in abi.OBJECT_PAYLOADS]
        self.assertEqual(domains, [1, 2, 3, 4, 5, 6])
        self.assertEqual(abi.decimal_dtype.itemsize, 16)

    def test_descriptor_fixtures_are_current(self):
        root = Path(__file__).resolve().parents[3]
        abi_fixture = (root / "fixtures" / "abi_descriptor.json").read_bytes()
        self.assertEqual(abi_fixture, abi.canonical_abi_descriptor())
        schema_fixture = (root / "fixtures" / "object_schema.json").read_bytes()
        self.assertEqual(schema_fixture, abi.object_schema_bytes())

    def test_object_schema_descriptor_shape(self):
        descriptor = abi.object_schema_descriptor()
        self.assertEqual(descriptor["object_schema_version"], 1)
        self.assertEqual(descriptor["object_ref_size"], 32)
        self.assertEqual(descriptor["context_size"], 440)
        payloads = descriptor["payloads"]
        self.assertEqual([payload["domain"] for payload in payloads], [1, 2, 3, 4, 5, 6])
        symbol = payloads[0]
        self.assertEqual(symbol["size"], 152)
        offsets = {field["name"]: field["offset"] for field in symbol["fields"]}
        self.assertEqual(offsets["price_tick"], 32)
        self.assertEqual(offsets["min_qty_lots"], 128)
        self.assertEqual(offsets["min_notional_tick_lots"], 136)
        # Payload major/minor are independently versioned from ABI_FINGERPRINT.
        self.assertEqual({payload["schema_minor"] for payload in payloads}, {abi.PAYLOAD_MINOR})


if __name__ == "__main__":
    unittest.main()
