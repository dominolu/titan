import json
from pathlib import Path
import tempfile
import unittest

from hftbacktest.data_manifest import DataManifest, load_data_manifest


class TestDataManifest(unittest.TestCase):
    def test_bar_kind_is_explicit_and_fully_described(self):
        manifest = DataManifest.from_dict(
            {
                "schema_version": 1,
                "data_kind": "bar",
                "symbol": "BTCUSDT",
                "venue": "binance",
                "timestamp_unit": "ns",
                "interval_semantics": "[open, close)",
                "timeframe_ns": 60_000_000_000,
                "bar_source": "canonical-local",
                "builder_version": "1",
            }
        )
        self.assertEqual(manifest.data_kind, "bar")

    def test_tick_cannot_silently_carry_bar_metadata(self):
        with self.assertRaises(ValueError):
            DataManifest.from_dict(
                {
                    "schema_version": 1,
                    "data_kind": "tick",
                    "symbol": "BTCUSDT",
                    "venue": "binance",
                    "timeframe_ns": 60,
                }
            )

    def test_json_sidecar_loader(self):
        value = {
            "schema_version": 1,
            "data_kind": "tick",
            "symbol": "BTCUSDT",
            "venue": "binance",
            "timestamp_unit": "ns",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trades.manifest.json"
            path.write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual(load_data_manifest(path).symbol, "BTCUSDT")


if __name__ == "__main__":
    unittest.main()
