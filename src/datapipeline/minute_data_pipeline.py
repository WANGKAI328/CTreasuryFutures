"""逐合约分钟线、T_mindf 初始化及历史分钟补丁。"""

from __future__ import annotations

from pathlib import Path
import time
from typing import Any, Sequence

import pandas as pd

from .data_pipeline import (
    HISTORICAL_MINUTE_COLUMNS,
    HISTORICAL_MINUTE_CSV,
    MINUTE_COLUMNS,
    MINUTE_TRADING_SESSIONS,
    MINUTE_WIND_START_CONTRACT,
    START_DATE,
    _as_date,
    _contract_dates,
    _filter_minute_wind_contracts,
    _normalise_historical_minute_chunk,
    fetch_minute_bars,
)
from .paths import (
    CONTRACTS_CSV_PATH,
    MAIN_MAPPING_CSV_PATH,
    MINUTE_CSV_DIR,
)
from .pipeline_io import (
    _atomic_write_csv,
    _contract_frame_for_update,
    _market_csv_path,
    _overlap_date_from_existing,
    _read_csv,
    _replace_market_csv_range,
    _summary_row,
)


def _mapping_trade_days(
    mapping: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp
) -> list[pd.Timestamp]:
    stored = pd.to_datetime(mapping.get("trade_date"), errors="coerce").dropna()
    days = {
        value.normalize()
        for value in stored
        if start.normalize() <= value.normalize() <= end.normalize()
    }
    fallback_start = (
        start.normalize() if not days else max(days) + pd.Timedelta(days=1)
    )
    days.update(pd.bdate_range(fallback_start, end.normalize()))
    return sorted(days)


def _session_timestamp(day: pd.Timestamp, clock: str) -> pd.Timestamp:
    return pd.Timestamp(f"{day.date()} {clock}")


def _minute_session_chunks(
    day: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for open_clock, close_clock in MINUTE_TRADING_SESSIONS:
        chunk_start = max(start, _session_timestamp(day, open_clock))
        chunk_end = min(end, _session_timestamp(day, close_clock))
        if chunk_start <= chunk_end:
            chunks.append((chunk_start, chunk_end))
    return chunks


def _latest_queryable_minute(
    mapping: pd.DataFrame, end_time: Any
) -> pd.Timestamp | None:
    """将请求终点裁剪到最近一个真实交易时段。"""
    end = pd.Timestamp(end_time)
    days = pd.to_datetime(mapping.get("trade_date"), errors="coerce").dropna()
    days = days.loc[days.dt.date <= end.date()].sort_values().drop_duplicates()
    if days.empty:
        return None
    day = days.iloc[-1].normalize()
    morning_open = _session_timestamp(day, MINUTE_TRADING_SESSIONS[0][0])
    morning_close = _session_timestamp(day, MINUTE_TRADING_SESSIONS[0][1])
    afternoon_open = _session_timestamp(day, MINUTE_TRADING_SESSIONS[1][0])
    afternoon_close = _session_timestamp(day, MINUTE_TRADING_SESSIONS[1][1])
    if day.date() < end.date():
        return afternoon_close
    if end < morning_open:
        if len(days) < 2:
            return None
        return _session_timestamp(
            days.iloc[-2].normalize(), MINUTE_TRADING_SESSIONS[-1][1]
        )
    if end <= morning_close:
        return end
    if end < afternoon_open:
        return morning_close
    return min(end, afternoon_close)


def _fetch_minute_range(
    wind: Any,
    mapping: pd.DataFrame,
    wind_code: str,
    start_time: Any,
    end_time: Any,
    retries: int,
    pause_seconds: float,
) -> pd.DataFrame:
    """按交易日和上午/下午交易时段拆分 Wind 分钟请求。"""
    start, end = pd.Timestamp(start_time), pd.Timestamp(end_time)
    pieces: list[pd.DataFrame] = []
    for day in _mapping_trade_days(mapping, start, end):
        for chunk_start, chunk_end in _minute_session_chunks(day, start, end):
            piece = fetch_minute_bars(
                wind, wind_code, chunk_start, chunk_end, retries
            )
            if not piece.empty:
                pieces.append(piece)
            if pause_seconds > 0:
                time.sleep(pause_seconds)
    if not pieces:
        return pd.DataFrame(columns=MINUTE_COLUMNS)
    return (
        pd.concat(pieces, ignore_index=True)
        .drop_duplicates(["wind_code", "bar_time"], keep="last")
        .sort_values(["wind_code", "bar_time"])
        .reset_index(drop=True)[MINUTE_COLUMNS]
    )


def update_minute_csvs(
    wind: Any,
    start_date: Any = START_DATE,
    end_time: Any | None = None,
    full: bool = False,
    overlap_trading_days: int = 2,
    contract_codes: Sequence[str] | None = None,
    minimum_contract: str = MINUTE_WIND_START_CONTRACT,
    contracts_csv_path: str | Path = CONTRACTS_CSV_PATH,
    mapping_csv_path: str | Path = MAIN_MAPPING_CSV_PATH,
    minute_dir: str | Path = MINUTE_CSV_DIR,
    retries: int = 3,
    pause_seconds: float = 0.0,
    continue_on_error: bool = False,
) -> pd.DataFrame:
    """每个合约写一个分钟 CSV；增量时回溯最近若干交易日。"""
    start = _as_date(start_date)
    requested_end = (
        pd.Timestamp.now().floor("s")
        if end_time is None
        else pd.Timestamp(end_time)
    )
    contracts = _read_csv(contracts_csv_path, "contracts")
    mapping = _read_csv(mapping_csv_path, "mapping")
    if contracts.empty or mapping.empty:
        raise FileNotFoundError(
            "分钟更新前必须先生成 contracts.csv 和 main_contract_mapping.csv"
        )
    selected = _contract_frame_for_update(
        contracts, start, requested_end.date(), full, contract_codes
    )
    selected = _filter_minute_wind_contracts(selected, minimum_contract)
    summary: list[dict[str, Any]] = []
    for number, row in selected.iterrows():
        code = str(row["wind_code"]).upper()
        path = _market_csv_path("minute", code, minute_dir=minute_dir)
        active_start_date, active_end_date = _contract_dates(
            row, start, requested_end.date()
        )
        active_start = pd.Timestamp(active_start_date)
        raw_end = min(
            pd.Timestamp(active_end_date)
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1),
            requested_end,
        )
        active_end = _latest_queryable_minute(mapping, raw_end)
        existing = _read_csv(path, "minute")
        query_start = active_start
        if not full and not existing.empty:
            wind_rows = existing.loc[
                existing["data_source"].fillna("wind").eq("wind")
            ]
            if not wind_rows.empty:
                overlap_date = _overlap_date_from_existing(
                    wind_rows["trade_date"],
                    overlap_trading_days,
                    active_start_date,
                )
                query_start = pd.Timestamp(overlap_date)
        if active_end is None or query_start > active_end:
            summary.append(
                _summary_row(
                    "minute",
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
            f"[minute csv {number + 1}/{len(selected)}] "
            f"{code}: {query_start} ~ {active_end}"
        )
        try:
            incoming = _fetch_minute_range(
                wind,
                mapping,
                code,
                query_start,
                active_end,
                retries,
                pause_seconds,
            )
            if incoming.empty:
                summary.append(
                    _summary_row(
                        "minute",
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
                "minute", path, incoming, query_start, active_end
            )
            summary.append(
                _summary_row(
                    "minute",
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
                    "minute",
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


def bootstrap_historical_minute_csvs(
    historical_csv: str | Path = HISTORICAL_MINUTE_CSV,
    minute_dir: str | Path = MINUTE_CSV_DIR,
    chunksize: int = 100_000,
) -> pd.DataFrame:
    """首次建库时将 T_mindf.csv 拆成每合约一个分钟 CSV。"""
    source = Path(historical_csv)
    if not source.is_file():
        raise FileNotFoundError(source)
    pieces_by_contract: dict[str, list[pd.DataFrame]] = {}
    for chunk_number, chunk in enumerate(
        pd.read_csv(
            source,
            usecols=list(HISTORICAL_MINUTE_COLUMNS),
            chunksize=chunksize,
        ),
        start=1,
    ):
        normalised = _normalise_historical_minute_chunk(chunk)
        for code, group in normalised.groupby("wind_code", sort=False):
            pieces_by_contract.setdefault(str(code), []).append(group)
        print(
            f"[T_mindf split {chunk_number}] valid={len(normalised):,}, "
            f"contracts={len(pieces_by_contract)}"
        )

    summary: list[dict[str, Any]] = []
    for code, pieces in sorted(pieces_by_contract.items()):
        incoming = (
            pd.concat(pieces, ignore_index=True)
            .drop_duplicates(["wind_code", "bar_time"], keep="last")
            .sort_values(["wind_code", "bar_time"])
        )
        path = _market_csv_path("minute", code, minute_dir=minute_dir)
        existing = _read_csv(path, "minute")
        if existing.empty:
            base = existing
        else:
            # 重跑初始化只替换 historical_csv 行，已有 Wind 行仍保持更高优先级。
            base = existing.loc[
                ~existing["data_source"].fillna("wind").eq("historical_csv")
            ]
        combined = (
            pd.concat([incoming, base], ignore_index=True)
            .drop_duplicates(["wind_code", "bar_time"], keep="last")
            .sort_values(["wind_code", "bar_time"])
            .reset_index(drop=True)[MINUTE_COLUMNS]
        )
        _atomic_write_csv(combined, path, MINUTE_COLUMNS)
        summary.append(
            _summary_row(
                "minute",
                code,
                incoming["bar_time"].min(),
                incoming["bar_time"].max(),
                len(incoming),
                len(combined),
                "ok",
                path,
            )
        )
    return pd.DataFrame(summary)


def stage_historical_minute_date_patch(
    patch_date: Any = "2024-11-21",
    historical_csv: str | Path = HISTORICAL_MINUTE_CSV,
    minute_dir: str | Path = MINUTE_CSV_DIR,
    chunksize: int = 100_000,
) -> pd.DataFrame:
    """将 T_mindf.csv 指定交易日先覆盖到分合约 CSV。"""
    target = _as_date(patch_date)
    pieces: list[pd.DataFrame] = []
    for chunk in pd.read_csv(
        historical_csv,
        usecols=list(HISTORICAL_MINUTE_COLUMNS),
        chunksize=chunksize,
    ):
        dates = pd.to_datetime(
            chunk["trading_date"], errors="coerce"
        ).dt.date
        selected = chunk.loc[dates.eq(target)]
        if not selected.empty:
            pieces.append(_normalise_historical_minute_chunk(selected))
    if not pieces:
        raise ValueError(f"T_mindf.csv 不包含 {target}")
    patch = (
        pd.concat(pieces, ignore_index=True)
        .drop_duplicates(["wind_code", "bar_time"], keep="last")
        .sort_values(["wind_code", "bar_time"])
    )
    summary: list[dict[str, Any]] = []
    day_start = pd.Timestamp(target)
    day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    for code, incoming in patch.groupby("wind_code", sort=True):
        path = _market_csv_path("minute", str(code), minute_dir=minute_dir)
        stored = _replace_market_csv_range(
            "minute", path, incoming, day_start, day_end
        )
        summary.append(
            _summary_row(
                "minute",
                str(code),
                day_start,
                day_end,
                len(incoming),
                stored["stored_rows"],
                "ok",
                path,
            )
        )
    return pd.DataFrame(summary)
