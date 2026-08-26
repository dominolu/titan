import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl


EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
sys.path.insert(0, str(EXAMPLES))

from dual_ma_bar_backtest import _select_daily_sources  # noqa: E402


def source_rows(day, source, count):
    start = datetime(*day, 9, 30, tzinfo=timezone.utc)
    return [
        {"ts": start + timedelta(minutes=index), "source": source}
        for index in range(count)
    ]


class TestDailySourceMerge(unittest.TestCase):
    def test_prefers_one_complete_source_per_session(self):
        frame = pl.DataFrame(
            source_rows((2024, 1, 2), "databento", 390)
            + source_rows((2024, 1, 3), "databento", 300)
            + source_rows((2024, 1, 3), "polygon_s3", 211)
        )
        merged, audit = _select_daily_sources(
            frame, ("polygon_s3", "databento", "s3")
        )
        self.assertEqual(len(merged), 601)
        self.assertEqual(audit["sessions"], 2)
        self.assertEqual(audit["daily_bar_counts"], {"211": 1, "390": 1})
        selected = {row["source"]: row["bars"] for row in audit["selected_source_coverage"]}
        self.assertEqual(selected, {"databento": 390, "polygon_s3": 211})

    def test_rejects_incomplete_selected_session(self):
        frame = pl.DataFrame(source_rows((2024, 1, 2), "databento", 200))
        with self.assertRaisesRegex(ValueError, "incomplete sessions"):
            _select_daily_sources(frame, ("polygon_s3", "databento", "s3"))


if __name__ == "__main__":
    unittest.main()
