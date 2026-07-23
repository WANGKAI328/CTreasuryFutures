"""合约目录和逐日主力映射的 Wind 下载与 CSV 增量维护。"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .data_pipeline import (
    CONTRACT_COLUMNS,
    END_DATE,
    MAPPING_COLUMNS,
    START_DATE,
    _as_date,
    fetch_contract_catalog,
    fetch_main_contract_mapping,
)
from .paths import CONTRACTS_CSV_PATH, MAIN_MAPPING_CSV_PATH
from .pipeline_io import (
    _atomic_write_csv,
    _merge_reference_frame,
    _read_csv,
)


def update_reference_csvs(
    wind: Any,
    start_date: Any = START_DATE,
    end_date: Any = END_DATE,
    full: bool = False,
    mapping_overlap_calendar_days: int = 10,
    contracts_csv_path: str | Path = CONTRACTS_CSV_PATH,
    mapping_csv_path: str | Path = MAIN_MAPPING_CSV_PATH,
    retries: int = 3,
) -> dict[str, Any]:
    """先将合约目录和主力映射保存为 CSV，再供 DuckDB 同步。"""
    start, end = _as_date(start_date), _as_date(end_date)
    existing_contracts = _read_csv(contracts_csv_path, "contracts")
    existing_mapping = _read_csv(mapping_csv_path, "mapping")

    max_mapping = (
        None if existing_mapping.empty else existing_mapping["trade_date"].max()
    )
    if full or max_mapping is None:
        mapping_start = start
    else:
        mapping_start = max(
            start,
            _as_date(max_mapping) - timedelta(days=mapping_overlap_calendar_days),
        )

    incoming_contracts = fetch_contract_catalog(wind, start, end, retries)
    incoming_mapping = fetch_main_contract_mapping(
        wind, mapping_start, end, retries
    )
    contracts = _merge_reference_frame(
        existing_contracts,
        incoming_contracts,
        ["wind_code"],
        CONTRACT_COLUMNS,
    )
    if existing_mapping.empty:
        mapping_base = existing_mapping
    else:
        mapping_dates = pd.to_datetime(
            existing_mapping["trade_date"]
        ).dt.date
        mapping_base = existing_mapping.loc[
            (mapping_dates < mapping_start) | (mapping_dates > end)
        ]
    mapping = _merge_reference_frame(
        mapping_base,
        incoming_mapping,
        ["trade_date"],
        MAPPING_COLUMNS,
    )
    _atomic_write_csv(contracts, contracts_csv_path, CONTRACT_COLUMNS)
    _atomic_write_csv(mapping, mapping_csv_path, MAPPING_COLUMNS)
    return {
        "contracts_rows": len(contracts),
        "mapping_rows": len(mapping),
        "mapping_start": mapping_start,
        "mapping_end": end,
        "contracts_csv": str(Path(contracts_csv_path).resolve()),
        "mapping_csv": str(Path(mapping_csv_path).resolve()),
    }
