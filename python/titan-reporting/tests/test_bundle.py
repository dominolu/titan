import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from titan_reporting import BundleError, load_bundle, render_html, render_quantstats


class BundleTest(unittest.TestCase):
    def test_verifies_digest_and_renders_without_recomputation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            content = json.dumps({"market_event_count": 5}).encode()
            (root / "result.json").write_bytes(content)
            manifest = {
                "schema_version": 1,
                "run_id": "run-1",
                "files": [{"path": "result.json", "bytes": len(content),
                           "sha256": hashlib.sha256(content).hexdigest()}],
            }
            (root / "manifest.json").write_text(json.dumps(manifest))
            bundle = load_bundle(root)
            output = render_html(bundle, root / "report.html")
            self.assertIn("market_event_count", output.read_text())
            (root / "result.json").write_text("{}")
            with self.assertRaises(BundleError):
                load_bundle(root)

    def test_rejects_unknown_schema_without_partial_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "manifest.json").write_text('{"schema_version": 999}')
            with self.assertRaises(BundleError):
                load_bundle(root)

    def test_quantstats_renders_truthful_no_data_page_for_empty_canonical_returns(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            content = json.dumps({"returns": []}).encode()
            (root / "result.json").write_bytes(content)
            manifest = {
                "schema_version": 1,
                "run_id": "run-empty",
                "files": [{"path": "result.json", "bytes": len(content),
                           "sha256": hashlib.sha256(content).hexdigest()}],
            }
            (root / "manifest.json").write_text(json.dumps(manifest))
            output = render_quantstats(load_bundle(root), root / "quantstats.html")
            page = output.read_text()
            self.assertIn("No canonical return observations", page)
            self.assertIn("No performance statistics were inferred", page)


if __name__ == "__main__":
    unittest.main()
