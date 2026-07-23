from __future__ import annotations

from datetime import date
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from datapipeline.duckdb_sync_pipeline import backup_duckdb, sync_csvs_to_duckdb
from datapipeline.eco_calendar_pipeline import (
    ECO_OUTPUT_COLUMNS,
    _classify_eco_indicator,
)
from datapipeline.pipeline_io import (
    _atomic_write_csv,
    _overlap_date_from_existing,
    _replace_market_csv_range,
)
from datapipeline.data_pipeline import (
    CONTRACT_COLUMNS,
    DAILY_COLUMNS,
    MAPPING_COLUMNS,
    MINUTE_COLUMNS,
    open_database,
)


class CsvFirstPipelineTest(unittest.TestCase):
    def test_event_classification_and_overlap(self) -> None:
        indicators = pd.Series(
            ["6月CPI:同比(%)", "6月社会融资规模", "第二季度GDP", "6月官方制造业PMI", "出口金额"]
        )
        classified = _classify_eco_indicator(indicators)
        self.assertEqual(
            classified.iloc[:4].tolist(),
            ["inflation", "credit", "growth", "pmi"],
        )
        self.assertTrue(pd.isna(classified.iloc[4]))
        dates = pd.date_range("2026-07-01", periods=8, freq="B")
        self.assertEqual(
            _overlap_date_from_existing(dates, 5, date(2020, 1, 1)),
            dates[-5].date(),
        )

    def test_csv_range_is_the_source_for_incremental_duckdb_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            contracts_path = root / "reference" / "contracts.csv"
            mapping_path = root / "reference" / "mapping.csv"
            eco_path = root / "reference" / "eco.csv"
            daily_path = root / "daily" / "T2609.CFE.csv"
            minute_path = root / "minute" / "T2609.CFE.csv"
            db_path = root / "database" / "test.duckdb"

            now = pd.Timestamp("2026-07-23 10:00:00")
            contracts = pd.DataFrame(
                [["10年期国债2609", "T2609", "T2609.CFE", "202609", 0.02, 0.02,
                  date(2026, 3, 16), date(2026, 9, 11), date(2026, 9, 1), now]],
                columns=CONTRACT_COLUMNS,
            )
            mapping = pd.DataFrame(
                [[date(2026, 7, 22), "T2609.CFE", now], [date(2026, 7, 23), "T2609.CFE", now]],
                columns=MAPPING_COLUMNS,
            )
            eco = pd.DataFrame(
                [[pd.Timestamp("2026-07-23"), "09:30", pd.Timestamp("2026-07-23 09:30"),
                  "中国", "6月CPI:同比(%)", "重要", 0.1, 0.2, 0.3, "inflation"]],
                columns=ECO_OUTPUT_COLUMNS,
            )
            daily = pd.DataFrame(
                [[date(2026, 7, 22), "T2609.CFE", 100.0, 101.0, 99.0, 100.5, 100.4,
                  1000.0, 1_000_000.0, 5000.0, 20.0, now]],
                columns=DAILY_COLUMNS,
            )
            minute = pd.DataFrame(
                [[date(2026, 7, 22), pd.Timestamp("2026-07-22 09:30"), "T2609.CFE",
                  100.0, 100.1, 99.9, 100.05, 10.0, 10_000.0, 5000.0,
                  pd.Timestamp("2026-07-22 09:30"), pd.Timestamp("2026-07-22 09:30:59"),
                  "wind", now]],
                columns=MINUTE_COLUMNS,
            )
            _atomic_write_csv(contracts, contracts_path, CONTRACT_COLUMNS)
            _atomic_write_csv(mapping, mapping_path, MAPPING_COLUMNS)
            _atomic_write_csv(eco, eco_path, ECO_OUTPUT_COLUMNS)
            _atomic_write_csv(daily, daily_path, DAILY_COLUMNS)
            _atomic_write_csv(minute, minute_path, MINUTE_COLUMNS)

            daily_summary = pd.DataFrame([{
                "dataset": "daily", "wind_code": "T2609.CFE",
                "start": "2026-07-22", "end": "2026-07-22",
                "incoming_rows": 1, "stored_rows": 1, "status": "ok",
                "csv_path": str(daily_path), "error": "",
            }])
            minute_summary = pd.DataFrame([{
                "dataset": "minute", "wind_code": "T2609.CFE",
                "start": "2026-07-22 09:30:00", "end": "2026-07-22 09:30:00",
                "incoming_rows": 1, "stored_rows": 1, "status": "ok",
                "csv_path": str(minute_path), "error": "",
            }])

            db_path.parent.mkdir(parents=True)
            con = open_database(db_path)
            try:
                result = sync_csvs_to_duckdb(
                    con, daily_summary, minute_summary,
                    date(2026, 7, 22), date(2026, 7, 23),
                    eco_csv_path=eco_path,
                    contracts_csv_path=contracts_path,
                    mapping_csv_path=mapping_path,
                )
                self.assertEqual(result["daily"]["rows"], 1)
                self.assertEqual(result["minute"]["rows"], 1)
                self.assertEqual(con.execute("SELECT count(*) FROM eco_calendar").fetchone()[0], 1)

                revised = daily.copy()
                revised.loc[0, "close"] = 100.8
                _replace_market_csv_range(
                    "daily", daily_path, revised,
                    pd.Timestamp("2026-07-22"), pd.Timestamp("2026-07-22"),
                )
                sync_csvs_to_duckdb(
                    con, daily_summary, pd.DataFrame(columns=minute_summary.columns),
                    date(2026, 7, 22), date(2026, 7, 23),
                    eco_csv_path=eco_path,
                    contracts_csv_path=contracts_path,
                    mapping_csv_path=mapping_path,
                )
                self.assertEqual(
                    con.execute("SELECT close FROM daily_bars").fetchone()[0],
                    100.8,
                )
                self.assertEqual(con.execute("SELECT count(*) FROM daily_bars").fetchone()[0], 1)
            finally:
                con.close()

            backup = backup_duckdb(db_path, root / "backups", label="test")
            self.assertIsNotNone(backup)
            self.assertTrue(backup.is_file())
            self.assertEqual(backup.stat().st_size, db_path.stat().st_size)


if __name__ == "__main__":
    unittest.main()
