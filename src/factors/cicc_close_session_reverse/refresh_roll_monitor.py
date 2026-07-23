"""刷新 DuckDB 移仓监控视图并记录质量检查结果。

从项目根目录直接运行：

    python src/factors/cicc_close_session_reverse/refresh_roll_monitor.py

安装过本项目后，也可以运行：

    python -m factors.cicc_close_session_reverse.refresh_roll_monitor
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any
from uuid import uuid4

import duckdb
import numpy as np
import pandas as pd

# 允许用户不安装项目，直接从项目根目录运行本文件。
if __package__ in {None, ""}:
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from factors.paths import DB_PATH, FACTOR_LOG_DIR, FACTOR_SQL_DIR
else:
    from ..paths import DB_PATH, FACTOR_LOG_DIR, FACTOR_SQL_DIR


DEFAULT_SQL_PATH = FACTOR_SQL_DIR / "roll_migration_monitor.sql"
VIEW_NAME = "v_roll_migration_monitor"
REQUIRED_TABLES = frozenset(
    {"contracts", "main_contract_mapping", "daily_bars"}
)


@dataclass(frozen=True, slots=True)
class RollMonitorRefreshResult:
    """一次移仓监控刷新的结果摘要。"""

    db_path: Path
    sql_path: Path
    log_dir: Path
    view_name: str
    rows: int
    min_trade_date: date | None
    max_trade_date: date | None
    latest_status: str | None
    latest_complete_date: date | None
    blocked_rows: int
    incomplete_rows: int
    status_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["db_path"] = str(self.db_path.resolve())
        payload["sql_path"] = str(self.sql_path.resolve())
        payload["log_dir"] = str(self.log_dir.resolve())
        return payload


def _json_default(value: Any) -> str | int | float | bool | None:
    if isinstance(value, Path):
        return str(value.resolve())
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if pd.isna(value):
        return None
    return str(value)


def _atomic_write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                indent=2,
                default=_json_default,
            ),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _create_logger(run_dir: Path) -> logging.Logger:
    run_dir.mkdir(parents=True, exist_ok=False)
    logger = logging.getLogger(f"roll_monitor_refresh.{run_dir.name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(
        run_dir / "refresh_roll_monitor.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def _close_logger(logger: logging.Logger) -> None:
    for handler in tuple(logger.handlers):
        handler.flush()
        handler.close()
        logger.removeHandler(handler)


def _validate_sources(con: duckdb.DuckDBPyConnection) -> None:
    available = {
        row[0]
        for row in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    }
    missing = sorted(REQUIRED_TABLES.difference(available))
    if missing:
        raise ValueError(f"DuckDB 缺少移仓监控源表: {missing}")

    empty = [
        table_name
        for table_name in sorted(REQUIRED_TABLES)
        if con.execute(
            f"SELECT count(*) FROM {table_name}"
        ).fetchone()[0]
        == 0
    ]
    if empty:
        raise ValueError(f"移仓监控源表为空: {empty}")


def _refresh_view(
    con: duckdb.DuckDBPyConnection,
    sql_path: Path,
) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    if "CREATE OR REPLACE VIEW" not in sql_text.upper():
        raise ValueError(f"SQL 未包含 CREATE OR REPLACE VIEW: {sql_path}")

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(sql_text)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def _collect_result(
    con: duckdb.DuckDBPyConnection,
    db_path: Path,
    sql_path: Path,
    log_dir: Path,
) -> RollMonitorRefreshResult:
    view_exists = con.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [VIEW_NAME],
    ).fetchone()[0]
    if view_exists != 1:
        raise RuntimeError(f"移仓监控视图未创建: {VIEW_NAME}")

    duplicate_dates = con.execute(
        f"""
        SELECT count(*)
        FROM (
            SELECT trade_date
            FROM {VIEW_NAME}
            GROUP BY trade_date
            HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_dates:
        raise ValueError(
            f"{VIEW_NAME} 存在 {duplicate_dates} 个重复交易日"
        )

    rows, minimum, maximum, blocked, incomplete = con.execute(
        f"""
        SELECT
            count(*),
            min(trade_date),
            max(trade_date),
            count(*) FILTER (WHERE block_signal),
            count(*) FILTER (WHERE NOT data_complete)
        FROM {VIEW_NAME}
        """
    ).fetchone()
    if rows == 0:
        raise ValueError(f"{VIEW_NAME} 为空")

    latest_status = con.execute(
        f"""
        SELECT roll_status
        FROM {VIEW_NAME}
        ORDER BY trade_date DESC
        LIMIT 1
        """
    ).fetchone()[0]
    latest_complete = con.execute(
        f"""
        SELECT max(trade_date)
        FROM {VIEW_NAME}
        WHERE data_complete
        """
    ).fetchone()[0]
    status_rows = con.execute(
        f"""
        SELECT roll_status, count(*) AS rows
        FROM {VIEW_NAME}
        GROUP BY roll_status
        ORDER BY roll_status
        """
    ).fetchall()
    status_counts = {
        str(status): int(status_count)
        for status, status_count in status_rows
    }
    return RollMonitorRefreshResult(
        db_path=db_path,
        sql_path=sql_path,
        log_dir=log_dir,
        view_name=VIEW_NAME,
        rows=int(rows),
        min_trade_date=minimum,
        max_trade_date=maximum,
        latest_status=str(latest_status),
        latest_complete_date=latest_complete,
        blocked_rows=int(blocked),
        incomplete_rows=int(incomplete),
        status_counts=status_counts,
    )


def refresh_roll_monitor(
    db_path: str | Path = DB_PATH,
    sql_path: str | Path = DEFAULT_SQL_PATH,
    log_root: str | Path = FACTOR_LOG_DIR,
) -> RollMonitorRefreshResult:
    """刷新移仓监控视图，完成质量检查并写入运行日志。"""
    database = Path(db_path)
    sql_file = Path(sql_path)
    run_id = (
        f"refresh_roll_monitor_"
        f"{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex[:8]}"
    )
    run_dir = Path(log_root) / run_id
    logger = _create_logger(run_dir)
    summary_path = run_dir / "run_summary.json"
    started_at = datetime.now()

    try:
        if not database.is_file():
            raise FileNotFoundError(f"DuckDB 不存在: {database}")
        if not sql_file.is_file():
            raise FileNotFoundError(f"移仓监控 SQL 不存在: {sql_file}")

        logger.info("开始刷新移仓监控: %s", database.resolve())
        with duckdb.connect(str(database)) as con:
            _validate_sources(con)
            _refresh_view(con, sql_file)
            result = _collect_result(
                con,
                database,
                sql_file,
                run_dir,
            )

        if result.incomplete_rows:
            logger.warning(
                "存在数据不完整交易日: %d；这些日期将按 fail-safe 屏蔽信号",
                result.incomplete_rows,
            )
        logger.info(
            "刷新完成: rows=%d, range=%s..%s, latest_status=%s",
            result.rows,
            result.min_trade_date,
            result.max_trade_date,
            result.latest_status,
        )
        logger.info(
            "质量摘要: latest_complete=%s, blocked=%d, statuses=%s",
            result.latest_complete_date,
            result.blocked_rows,
            result.status_counts,
        )
        _atomic_write_json(
            {
                "status": "success",
                "started_at": started_at,
                "finished_at": datetime.now(),
                **result.to_dict(),
            },
            summary_path,
        )
        return result
    except Exception as exc:
        logger.exception("移仓监控刷新失败: %s", exc)
        _atomic_write_json(
            {
                "status": "error",
                "started_at": started_at,
                "finished_at": datetime.now(),
                "db_path": database,
                "sql_path": sql_file,
                "log_dir": run_dir,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            summary_path,
        )
        raise
    finally:
        _close_logger(logger)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="刷新并检查 CICC 尾盘反转因子的移仓监控视图"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DB_PATH,
        help=f"DuckDB 路径，默认：{DB_PATH}",
    )
    parser.add_argument(
        "--sql-path",
        type=Path,
        default=DEFAULT_SQL_PATH,
        help=f"移仓监控 SQL，默认：{DEFAULT_SQL_PATH}",
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        default=FACTOR_LOG_DIR,
        help=f"日志根目录，默认：{FACTOR_LOG_DIR}",
    )
    return parser.parse_args(argv)


def configure_console_encoding() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8")


def main(argv: list[str] | None = None) -> RollMonitorRefreshResult:
    configure_console_encoding()
    args = parse_args(argv)
    result = refresh_roll_monitor(
        db_path=args.db_path,
        sql_path=args.sql_path,
        log_root=args.log_root,
    )
    print(
        json.dumps(
            result.to_dict(),
            ensure_ascii=False,
            indent=2,
            default=_json_default,
        )
    )
    return result


if __name__ == "__main__":
    main()
