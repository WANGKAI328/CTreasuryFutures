"""逐合约日线的 Wind 下载、五交易日回溯和原子 CSV 更新。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from .data_pipeline import (
    END_DATE,
    START_DATE,
    _as_date,
    _contract_dates,
    fetch_daily_bars,
)
from .paths import CONTRACTS_CSV_PATH, DAILY_CSV_DIR
from .pipeline_io import (
    _contract_frame_for_update,
    _market_csv_path,
    _overlap_date_from_existing,
    _read_csv,
    _replace_market_csv_range,
    _summary_row,
)


def update_daily_csvs(
    wind: Any,
    start_date: Any = START_DATE,
    end_date: Any = END_DATE,
    full: bool = False,
    overlap_trading_days: int = 5,
    contract_codes: Sequence[str] | None = None,
    contracts_csv_path: str | Path = CONTRACTS_CSV_PATH,
    daily_dir: str | Path = DAILY_CSV_DIR,
    retries: int = 3,
    continue_on_error: bool = False,
) -> pd.DataFrame:
    """每个合约写一个日线 CSV；增量时回溯最近若干交易日。"""
    start, end = _as_date(start_date), _as_date(end_date)
    contracts = _read_csv(contracts_csv_path, "contracts")
    if contracts.empty:
        raise FileNotFoundError(f"合约目录 CSV 为空: {contracts_csv_path}")
    selected = _contract_frame_for_update(
        contracts, start, end, full, contract_codes
    )
    summary: list[dict[str, Any]] = []
    for number, row in selected.iterrows():
        code = str(row["wind_code"]).upper()
        path = _market_csv_path("daily", code, daily_dir=daily_dir)
        active_start, active_end = _contract_dates(row, start, end)
        existing = _read_csv(path, "daily")
        query_start = active_start
        if not full and not existing.empty:
            query_start = _overlap_date_from_existing(
                existing["trade_date"], overlap_trading_days, active_start
            )
        if query_start > active_end:
            summary.append(
                _summary_row(
                    "daily",
                    code,
                    query_start,
                    active_end,
                    0,
                    len(existing),
                    "up_to_date",
                    path,
                )
            )
            continue
        print(
            f"[daily csv {number + 1}/{len(selected)}] "
            f"{code}: {query_start} ~ {active_end}"
        )
        try:
            incoming = fetch_daily_bars(
                wind, code, query_start, active_end, retries
            )
            if incoming.empty:
                summary.append(
                    _summary_row(
                        "daily",
                        code,
                        query_start,
                        active_end,
                        0,
                        len(existing),
                        "no_data",
                        path,
                    )
                )
                continue
            stored = _replace_market_csv_range(
                "daily", path, incoming, query_start, active_end
            )
            summary.append(
                _summary_row(
                    "daily",
                    code,
                    query_start,
                    active_end,
                    len(incoming),
                    stored["stored_rows"],
                    "ok",
                    path,
                )
            )
        except Exception as exc:
            summary.append(
                _summary_row(
                    "daily",
                    code,
                    query_start,
                    active_end,
                    0,
                    len(existing),
                    "error",
                    path,
                    str(exc),
                )
            )
            if not continue_on_error:
                raise
    return pd.DataFrame(summary)
