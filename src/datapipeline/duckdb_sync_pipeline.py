"""DuckDB 备份及从标准 CSV 到数据库的事务化增量同步。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import shutil
from typing import Any
from uuid import uuid4

import duckdb
import pandas as pd

from .data_pipeline import (
    CONTRACT_COLUMNS,
    DAILY_COLUMNS,
    MAPPING_COLUMNS,
    MINUTE_COLUMNS,
    _as_date,
    _upsert_frame,
)
from .paths import (
    CONTRACTS_CSV_PATH,
    DB_PATH,
    DUCKDB_BACKUP_DIR,
    ECO_CALENDAR_CSV_PATH,
    MAIN_MAPPING_CSV_PATH,
)
from .pipeline_io import _read_csv


ECO_DB_COLUMNS = [
    "event_date",
    "event_time",
    "event_datetime",
    "region",
    "indicator",
    "importance",
    "prev",
    "forecast",
    "actual",
    "tf_category",
    "ingested_at",
]


def backup_duckdb(
    db_path: str | Path = DB_PATH,
    backup_dir: str | Path = DUCKDB_BACKUP_DIR,
    label: str = "before_update",
) -> Path | None:
    """写库前创建时间戳备份，并验证备份可以只读打开。"""
    source = Path(db_path)
    if not source.is_file():
        return None
    destination_dir = Path(backup_dir)
    destination_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination = destination_dir / (
        f"{source.stem}_{stamp}_{label}_{uuid4().hex[:8]}{source.suffix}"
    )
    shutil.copy2(source, destination)
    if destination.stat().st_size != source.stat().st_size:
        destination.unlink(missing_ok=True)
        raise IOError("DuckDB 备份大小与源文件不一致")
    check = duckdb.connect(str(destination), read_only=True)
    try:
        check.execute("SELECT 1").fetchone()
    finally:
        check.close()
    return destination


def _eco_frame_for_database(path: str | Path) -> pd.DataFrame:
    source = _read_csv(path, "eco")
    if source.empty:
        raise ValueError(f"经济日历 CSV 为空: {path}")
    out = source.rename(
        columns={
            "date": "event_date",
            "time": "event_time",
            "datetime": "event_datetime",
        }
    ).copy()
    out["event_date"] = pd.to_datetime(
        out["event_date"], errors="coerce"
    ).dt.date
    out["event_datetime"] = pd.to_datetime(
        out["event_datetime"], errors="coerce"
    )
    out["ingested_at"] = pd.Timestamp.now()
    missing_key = out[
        ["event_date", "event_datetime", "region", "indicator"]
    ].isna().any(axis=1)
    if missing_key.any():
        raise ValueError("经济日历 CSV 存在空主键")
    return out[ECO_DB_COLUMNS]


def _coalesce_summary_ranges(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return summary.copy()
    valid = summary.loc[summary["status"].eq("ok")].copy()
    if valid.empty:
        return valid
    valid["start"] = pd.to_datetime(valid["start"], errors="coerce")
    valid["end"] = pd.to_datetime(valid["end"], errors="coerce")
    if valid[["start", "end"]].isna().any().any():
        raise ValueError("更新摘要中存在无法解析的 start/end")
    return (
        valid.groupby(
            ["dataset", "wind_code", "csv_path"], as_index=False
        )
        .agg(start=("start", "min"), end=("end", "max"))
        .sort_values(["dataset", "wind_code"])
        .reset_index(drop=True)
    )


def _sync_market_ranges(
    con: duckdb.DuckDBPyConnection,
    summary: pd.DataFrame,
    dataset: str,
) -> dict[str, Any]:
    table = "daily_bars" if dataset == "daily" else "minute_bars"
    columns = DAILY_COLUMNS if dataset == "daily" else MINUTE_COLUMNS
    timestamp_column = "trade_date" if dataset == "daily" else "bar_time"
    ranges = _coalesce_summary_ranges(summary)
    if not ranges.empty:
        ranges = ranges.loc[ranges["dataset"].eq(dataset)]
    rows = 0
    affected_start: pd.Timestamp | None = None
    affected_end: pd.Timestamp | None = None
    for item in ranges.itertuples(index=False):
        source = _read_csv(item.csv_path, dataset)
        if source.empty:
            raise ValueError(f"待同步 CSV 为空: {item.csv_path}")
        values = pd.to_datetime(source[timestamp_column], errors="coerce")
        batch = source.loc[
            values.between(item.start, item.end, inclusive="both")
        ]
        batch = batch.loc[batch["wind_code"].eq(item.wind_code)]
        if batch.empty:
            raise ValueError(
                f"{item.wind_code} CSV 在同步区间没有数据: "
                f"{item.start}~{item.end}"
            )
        con.execute(
            f"DELETE FROM {table} WHERE wind_code = ? "
            f"AND {timestamp_column} BETWEEN ? AND ?",
            [item.wind_code, item.start, item.end],
        )
        rows += _upsert_frame(con, table, batch, columns)
        affected_start = (
            item.start
            if affected_start is None
            else min(affected_start, item.start)
        )
        affected_end = (
            item.end if affected_end is None else max(affected_end, item.end)
        )
    return {
        "ranges": len(ranges),
        "rows": rows,
        "affected_start": affected_start,
        "affected_end": affected_end,
    }


def sync_csvs_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    daily_summary: pd.DataFrame,
    minute_summary: pd.DataFrame,
    mapping_start: Any,
    mapping_end: Any,
    eco_csv_path: str | Path = ECO_CALENDAR_CSV_PATH,
    contracts_csv_path: str | Path = CONTRACTS_CSV_PATH,
    mapping_csv_path: str | Path = MAIN_MAPPING_CSV_PATH,
) -> dict[str, Any]:
    """在一个事务内同步参考数据和本次变化的行情 CSV 区间。"""
    contracts = _read_csv(contracts_csv_path, "contracts")
    mapping = _read_csv(mapping_csv_path, "mapping")
    eco = _eco_frame_for_database(eco_csv_path)
    if contracts.empty or mapping.empty:
        raise ValueError("参考数据 CSV 为空，拒绝更新 DuckDB")
    map_start, map_end = _as_date(mapping_start), _as_date(mapping_end)
    mapping_dates = pd.to_datetime(
        mapping["trade_date"], errors="coerce"
    ).dt.date
    mapping_batch = mapping.loc[
        mapping_dates.ge(map_start) & mapping_dates.le(map_end)
    ]
    if mapping_batch.empty:
        raise ValueError(f"主力映射 CSV 在 {map_start}~{map_end} 没有数据")

    con.execute("BEGIN TRANSACTION")
    try:
        contract_rows = _upsert_frame(
            con, "contracts", contracts, CONTRACT_COLUMNS
        )
        con.execute(
            "DELETE FROM main_contract_mapping "
            "WHERE trade_date BETWEEN ? AND ?",
            [map_start, map_end],
        )
        mapping_rows = _upsert_frame(
            con,
            "main_contract_mapping",
            mapping_batch,
            MAPPING_COLUMNS,
        )
        # 经济日历 CSV 是完整快照，因此先删除 CSV 中已不存在的旧事件。
        con.register("_eco_snapshot", eco)
        try:
            con.execute(
                """
                DELETE FROM eco_calendar AS stored
                WHERE NOT EXISTS (
                    SELECT 1 FROM _eco_snapshot AS incoming
                    WHERE incoming.event_datetime = stored.event_datetime
                      AND incoming.region = stored.region
                      AND incoming.indicator = stored.indicator
                )
                """
            )
        finally:
            con.unregister("_eco_snapshot")
        eco_rows = _upsert_frame(con, "eco_calendar", eco, ECO_DB_COLUMNS)
        daily_result = _sync_market_ranges(con, daily_summary, "daily")
        minute_result = _sync_market_ranges(con, minute_summary, "minute")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return {
        "contracts_rows": contract_rows,
        "mapping_rows": mapping_rows,
        "eco_rows": eco_rows,
        "daily": daily_result,
        "minute": minute_result,
    }
