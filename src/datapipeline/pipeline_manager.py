"""CSV-first 管线统一管理器：编排全量、增量和补丁任务。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from .daily_data_pipeline import update_daily_csvs
from .data_pipeline import (
    START_DATE,
    _as_date,
    build_adj_factor_list,
    build_main_daily_continuous,
    build_main_minute_continuous,
    open_database,
)
from .duckdb_sync_pipeline import backup_duckdb, sync_csvs_to_duckdb
from .eco_calendar_pipeline import build_eco_calendar_files
from .minute_data_pipeline import (
    bootstrap_historical_minute_csvs,
    stage_historical_minute_date_patch,
    update_minute_csvs,
)
from .paths import DB_PATH
from .pipeline_io import (
    MarketDataset,
    PipelineRunLog,
    _combine_summaries,
    _raise_update_errors,
)
from .reference_data_pipeline import update_reference_csvs


def _minimum_changed_date(
    mapping_start: Any, market_result: dict[str, Any]
) -> Any:
    candidate = market_result.get("affected_start")
    return min(
        _as_date(mapping_start),
        _as_date(candidate) if candidate is not None else _as_date(mapping_start),
    )


def run_full_initialization(
    wind: Any,
    start_date: Any = START_DATE,
    end_time: Any | None = None,
    historical_patch_dates: Sequence[Any] = ("2024-11-21",),
    db_path: str | Path = DB_PATH,
    retries: int = 3,
    pause_seconds: float = 0.0,
) -> dict[str, Any]:
    """首次建库：原始 Excel/T_mindf/Wind -> CSV -> 备份 -> DuckDB。"""
    log = PipelineRunLog("full")
    end = (
        pd.Timestamp.now().floor("s")
        if end_time is None
        else pd.Timestamp(end_time)
    )
    start = _as_date(start_date)
    try:
        eco_result = build_eco_calendar_files()
        log.event("eco_calendar_built", **eco_result)

        reference = update_reference_csvs(
            wind, start, end.date(), full=True, retries=retries
        )
        log.event("reference_csvs_updated", **reference)

        historical = bootstrap_historical_minute_csvs()
        log.save_frame("historical_minute_split", historical)

        daily = update_daily_csvs(
            wind,
            start,
            end.date(),
            full=True,
            retries=retries,
            continue_on_error=True,
        )
        log.save_frame("daily_updates", daily)
        _raise_update_errors(daily, "日线 CSV 更新")

        minute_wind = update_minute_csvs(
            wind,
            start,
            end,
            full=True,
            retries=retries,
            pause_seconds=pause_seconds,
            continue_on_error=True,
        )
        log.save_frame("minute_wind_updates", minute_wind)
        _raise_update_errors(minute_wind, "分钟 CSV 更新")

        patches = [
            stage_historical_minute_date_patch(patch_date)
            for patch_date in historical_patch_dates
        ]
        patch_summary = _combine_summaries(*patches)
        if not patch_summary.empty:
            log.save_frame("historical_minute_patches", patch_summary)
        minute = _combine_summaries(historical, minute_wind, patch_summary)

        backup = backup_duckdb(db_path, label="before_full")
        log.event("duckdb_backup_created", backup_path=backup)
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        con = open_database(db_path)
        try:
            sync = sync_csvs_to_duckdb(
                con,
                daily,
                minute,
                reference["mapping_start"],
                reference["mapping_end"],
            )
            continuous = {
                "daily": build_main_daily_continuous(
                    con, start, end.date()
                ),
                "minute": build_main_minute_continuous(
                    con, start, end.date()
                ),
            }
            factors = build_adj_factor_list(con, strict=True)
        finally:
            con.close()
        result = {
            "run_id": log.run_id,
            "log_dir": str(log.run_dir.resolve()),
            "backup_path": str(backup.resolve()) if backup else None,
            "eco": eco_result,
            "reference": reference,
            "duckdb_sync": sync,
            "continuous": continuous,
            "factor_rows": len(factors),
        }
        log.finish("success", **result)
        return result
    except Exception as exc:
        log.event("run_failed", level="ERROR", error=str(exc))
        log.finish("error", error=str(exc))
        raise


def run_incremental_update(
    wind: Any,
    start_date: Any = START_DATE,
    end_time: Any | None = None,
    daily_overlap_trading_days: int = 5,
    minute_overlap_trading_days: int = 2,
    db_path: str | Path = DB_PATH,
    retries: int = 3,
    pause_seconds: float = 0.0,
) -> dict[str, Any]:
    """日常更新：先重叠更新活跃合约 CSV，再同步变化区间。"""
    log = PipelineRunLog("incremental")
    end = (
        pd.Timestamp.now().floor("s")
        if end_time is None
        else pd.Timestamp(end_time)
    )
    start = _as_date(start_date)
    try:
        if not Path(db_path).is_file():
            raise FileNotFoundError(f"DuckDB 不存在，请先全量初始化: {db_path}")

        eco_result = build_eco_calendar_files()
        log.event("eco_calendar_built", **eco_result)
        reference = update_reference_csvs(
            wind, start, end.date(), full=False, retries=retries
        )
        log.event("reference_csvs_updated", **reference)

        daily = update_daily_csvs(
            wind,
            start,
            end.date(),
            full=False,
            overlap_trading_days=daily_overlap_trading_days,
            retries=retries,
            continue_on_error=True,
        )
        log.save_frame("daily_updates", daily)
        _raise_update_errors(daily, "日线 CSV 更新")

        minute = update_minute_csvs(
            wind,
            start,
            end,
            full=False,
            overlap_trading_days=minute_overlap_trading_days,
            retries=retries,
            pause_seconds=pause_seconds,
            continue_on_error=True,
        )
        log.save_frame("minute_updates", minute)
        _raise_update_errors(minute, "分钟 CSV 更新")

        backup = backup_duckdb(db_path, label="before_incremental")
        log.event("duckdb_backup_created", backup_path=backup)
        con = open_database(db_path)
        try:
            sync = sync_csvs_to_duckdb(
                con,
                daily,
                minute,
                reference["mapping_start"],
                reference["mapping_end"],
            )
            daily_start = _minimum_changed_date(
                reference["mapping_start"], sync["daily"]
            )
            minute_start = _minimum_changed_date(
                reference["mapping_start"], sync["minute"]
            )
            continuous = {
                "daily": build_main_daily_continuous(
                    con, daily_start, end.date()
                ),
                "minute": build_main_minute_continuous(
                    con, minute_start, end.date()
                ),
            }
            factors = build_adj_factor_list(con, strict=True)
        finally:
            con.close()
        result = {
            "run_id": log.run_id,
            "log_dir": str(log.run_dir.resolve()),
            "backup_path": str(backup.resolve()) if backup else None,
            "eco": eco_result,
            "reference": reference,
            "duckdb_sync": sync,
            "continuous": continuous,
            "factor_rows": len(factors),
        }
        log.finish("success", **result)
        return result
    except Exception as exc:
        log.event("run_failed", level="ERROR", error=str(exc))
        log.finish("error", error=str(exc))
        raise


def run_historical_minute_patch(
    patch_date: Any = "2024-11-21",
    db_path: str | Path = DB_PATH,
) -> dict[str, Any]:
    """统一管理 T_mindf 单日补丁：CSV -> 备份 -> DuckDB。"""
    log = PipelineRunLog("historical_patch")
    target = _as_date(patch_date)
    try:
        if not Path(db_path).is_file():
            raise FileNotFoundError(f"DuckDB 不存在，不能执行补丁: {db_path}")
        patch = stage_historical_minute_date_patch(target)
        log.save_frame("historical_minute_patch", patch)
        backup = backup_duckdb(
            db_path, label=f"before_patch_{target:%Y%m%d}"
        )
        log.event("duckdb_backup_created", backup_path=backup)
        con = open_database(db_path)
        try:
            sync = sync_csvs_to_duckdb(
                con,
                pd.DataFrame(columns=patch.columns),
                patch,
                target,
                target,
            )
            continuous = build_main_minute_continuous(con, target, target)
        finally:
            con.close()
        result = {
            "run_id": log.run_id,
            "log_dir": str(log.run_dir.resolve()),
            "backup_path": str(backup.resolve()) if backup else None,
            "patch_date": target,
            "patched_contracts": len(patch),
            "duckdb_sync": sync,
            "continuous": continuous,
        }
        log.finish("success", **result)
        return result
    except Exception as exc:
        log.event("run_failed", level="ERROR", error=str(exc))
        log.finish("error", error=str(exc))
        raise


def run_contract_wind_patch(
    wind: Any,
    dataset: MarketDataset,
    wind_code: str,
    start: Any,
    end: Any,
    db_path: str | Path = DB_PATH,
    retries: int = 3,
    pause_seconds: float = 0.0,
) -> dict[str, Any]:
    """统一管理指定合约 Wind 补丁：Wind -> CSV -> 备份 -> DuckDB。"""
    if dataset not in {"daily", "minute"}:
        raise ValueError("dataset 只能是 'daily' 或 'minute'")
    log = PipelineRunLog(f"{dataset}_wind_patch")
    code = str(wind_code).strip().upper()
    start_timestamp, end_timestamp = pd.Timestamp(start), pd.Timestamp(end)
    if start_timestamp > end_timestamp:
        raise ValueError("补丁 start 不能晚于 end")
    try:
        if not Path(db_path).is_file():
            raise FileNotFoundError(f"DuckDB 不存在，不能执行补丁: {db_path}")
        if dataset == "daily":
            patch = update_daily_csvs(
                wind,
                start_timestamp.date(),
                end_timestamp.date(),
                full=True,
                contract_codes=[code],
                retries=retries,
                continue_on_error=True,
            )
            daily, minute = patch, pd.DataFrame(columns=patch.columns)
        else:
            patch = update_minute_csvs(
                wind,
                start_timestamp.date(),
                end_timestamp,
                full=True,
                contract_codes=[code],
                retries=retries,
                pause_seconds=pause_seconds,
                continue_on_error=True,
            )
            daily, minute = pd.DataFrame(columns=patch.columns), patch

        log.save_frame(f"{dataset}_patch", patch)
        _raise_update_errors(patch, f"{code} {dataset} 补丁")
        if patch.empty or not patch["status"].eq("ok").any():
            raise ValueError(f"{code} 补丁没有取得可写入数据")

        backup = backup_duckdb(
            db_path,
            label=f"before_{dataset}_patch_{code.replace('.', '_')}",
        )
        log.event("duckdb_backup_created", backup_path=backup)
        con = open_database(db_path)
        try:
            sync = sync_csvs_to_duckdb(
                con,
                daily,
                minute,
                start_timestamp.date(),
                end_timestamp.date(),
            )
            if dataset == "daily":
                continuous = build_main_daily_continuous(
                    con, start_timestamp.date(), end_timestamp.date()
                )
                factor_rows: int | None = len(
                    build_adj_factor_list(con, strict=True)
                )
            else:
                continuous = build_main_minute_continuous(
                    con, start_timestamp.date(), end_timestamp.date()
                )
                factor_rows = None
        finally:
            con.close()
        result = {
            "run_id": log.run_id,
            "log_dir": str(log.run_dir.resolve()),
            "backup_path": str(backup.resolve()) if backup else None,
            "dataset": dataset,
            "wind_code": code,
            "start": start_timestamp,
            "end": end_timestamp,
            "duckdb_sync": sync,
            "continuous": continuous,
            "factor_rows": factor_rows,
        }
        log.finish("success", **result)
        return result
    except Exception as exc:
        log.event("run_failed", level="ERROR", error=str(exc))
        log.finish("error", error=str(exc))
        raise
