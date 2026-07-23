"""CSV-first 国债期货数据管线命令行入口。

这个脚本对应同目录下的 ``CSV_First_Data_Pipeline.ipynb``，将 Notebook
中的运行开关改成了更安全的 ``--mode`` 参数。

常用示例（从项目根目录执行）：

    # 只构建经济日历，不连接 Wind、不修改 DuckDB
    python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode eco

    # 第一次全量建库
    python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode full

    # 日常增量更新
    python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode incremental

    # 用 T_mindf.csv 修复指定交易日
    python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode historical-patch --historical-patch-date 2024-11-21

    # 强制重抓指定合约的日线区间
    python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode wind-patch --dataset daily --contract T2609.CFE --patch-start 2026-07-01 --patch-end 2026-07-23

    # 只读审计数据库
    python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode audit
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any


# 自动向上寻找项目根目录，保证从项目内任意目录启动时都能正确导入 src。
_PROJECT_CANDIDATES = (Path.cwd(), *Path.cwd().parents, Path(__file__).resolve().parent)
PROJECT_ROOT = next(
    (
        candidate
        for base in _PROJECT_CANDIDATES
        for candidate in (base, *base.parents)
        if (candidate / "pyproject.toml").is_file()
    ),
    None,
)
if PROJECT_ROOT is None:
    raise FileNotFoundError("找不到项目根目录 pyproject.toml")

SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import duckdb
import pandas as pd

from datapipeline.eco_calendar_pipeline import build_eco_calendar_files
from datapipeline.pipeline_manager import (
    run_contract_wind_patch,
    run_full_initialization,
    run_historical_minute_patch,
    run_incremental_update,
)
from datapipeline.paths import (
    CSV_DATA_DIR,
    DATA_PIPELINE_LOG_DIR,
    DB_PATH,
    DUCKDB_BACKUP_DIR,
)


DEFAULT_START_DATE = "2015-06-30" # 默认开始时间
DEFAULT_DAILY_OVERLAP = 5 # 日线下载时的回溯交易日数, 回溯是因为 Wind 的日线数据可能会有延迟，导致最新的几天数据不完整
DEFAULT_MINUTE_OVERLAP = 2 # 分钟线下载时的回溯交易日数, 回溯是因为 Wind 的分钟线数据可能会有延迟，导致最新的几天数据不完整
DEFAULT_HISTORICAL_PATCH_DATE = "2024-11-21" # 默认历史补丁日期


def configure_console_encoding() -> None:
    """统一终端输出为 UTF-8，避免 Windows 重定向输出时中文乱码。"""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8")


def parse_args() -> argparse.Namespace:
    """解析命令行参数；一次运行只允许选择一种模式。"""
    parser = argparse.ArgumentParser(
        description="CSV-first 国债期货全量、增量、补丁与审计入口",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=(
            "eco",
            "full",
            "incremental",
            "historical-patch",
            "wind-patch",
            "audit",
        ),
        help=(
            "eco=经济日历；full=首次建库；incremental=日常更新；"
            "historical-patch=T_mindf 日期补丁；wind-patch=指定 Wind 区间补丁；"
            "audit=只读审计"
        ),
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help=f"全量历史起点，默认 {DEFAULT_START_DATE}",
    )
    parser.add_argument(
        "--end-time",
        default=None,
        help="全量或增量运行终点；不传则使用启动脚本时的当前时间",
    )
    parser.add_argument(
        "--daily-overlap",
        type=int,
        default=DEFAULT_DAILY_OVERLAP,
        help=f"日线回溯交易日数，默认 {DEFAULT_DAILY_OVERLAP}",
    )
    parser.add_argument(
        "--minute-overlap",
        type=int,
        default=DEFAULT_MINUTE_OVERLAP,
        help=f"分钟线回溯交易日数，默认 {DEFAULT_MINUTE_OVERLAP}",
    )
    parser.add_argument(
        "--historical-patch-date",
        default=DEFAULT_HISTORICAL_PATCH_DATE,
        help=f"T_mindf 历史分钟补丁日期，默认 {DEFAULT_HISTORICAL_PATCH_DATE}",
    )
    parser.add_argument(
        "--dataset",
        choices=("daily", "minute"),
        default="daily",
        help="Wind 补丁的数据类型，默认 daily",
    )
    parser.add_argument("--contract", help="Wind 补丁合约，例如 T2609.CFE")
    parser.add_argument("--patch-start", help="Wind 补丁起点")
    parser.add_argument("--patch-end", help="Wind 补丁终点")
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.0,
        help="分钟 Wind 请求之间的暂停秒数，默认 0",
    )
    return parser.parse_args()


def start_wind() -> Any:
    """只在需要 Wind 的模式中导入并连接 WindPy。"""
    from WindPy import w

    w.start(waitTime=60)
    if not w.isconnected():
        raise ConnectionError("WindPy 未连接")
    return w


def print_result(result: Any) -> None:
    """以适合终端阅读的形式输出函数返回结果。"""
    if isinstance(result, pd.DataFrame):
        print(result.to_string(index=False))
    elif isinstance(result, dict):
        print(pd.Series(result, name="result").to_string())
    else:
        print(result)


def audit_database(db_path: Path = DB_PATH) -> pd.DataFrame:
    """只读检查当前数据库中各张核心表的行数和覆盖范围。"""
    if not db_path.is_file():
        raise FileNotFoundError(db_path)

    # 旧库可能尚未创建 eco_calendar，因此先读取实际存在的表名。
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        existing = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        definitions = [
            ("eco_calendar", "event_datetime"),
            ("contracts", None),
            ("main_contract_mapping", "trade_date"),
            ("daily_bars", "trade_date"),
            ("minute_bars", "bar_time"),
            ("main_daily_continuous", "trade_date"),
            ("main_minute_continuous", "bar_time"),
            ("main_adj_factor_list", "segment_start"),
        ]
        rows: list[dict[str, Any]] = []
        for table_name, time_column in definitions:
            if table_name not in existing:
                rows.append(
                    {
                        "table_name": table_name,
                        "rows": 0,
                        "min_time": None,
                        "max_time": None,
                        "status": "not_created",
                    }
                )
                continue
            if time_column is None:
                count = con.execute(
                    f"SELECT count(*) FROM {table_name}"
                ).fetchone()[0]
                minimum, maximum = None, None
            else:
                count, minimum, maximum = con.execute(
                    f"SELECT count(*), min({time_column}), max({time_column}) FROM {table_name}"
                ).fetchone()
            rows.append(
                {
                    "table_name": table_name,
                    "rows": count,
                    "min_time": minimum,
                    "max_time": maximum,
                    "status": "ok",
                }
            )
        return pd.DataFrame(rows)
    finally:
        # 无论查询是否成功都释放只读连接，避免影响之后的写入任务。
        con.close()


def validate_args(args: argparse.Namespace) -> None:
    """在连接 Wind 或写文件前完成参数检查。"""
    if args.daily_overlap < 1:
        raise ValueError("--daily-overlap 必须大于等于 1")
    if args.minute_overlap < 1:
        raise ValueError("--minute-overlap 必须大于等于 1")
    if args.pause_seconds < 0:
        raise ValueError("--pause-seconds 不能为负数")
    if args.mode == "wind-patch":
        missing = [
            name
            for name, value in (
                ("--contract", args.contract),
                ("--patch-start", args.patch_start),
                ("--patch-end", args.patch_end),
            )
            if not value
        ]
        if missing:
            raise ValueError("wind-patch 缺少参数: " + ", ".join(missing))
        if pd.Timestamp(args.patch_start) > pd.Timestamp(args.patch_end):
            raise ValueError("--patch-start 不能晚于 --patch-end")


def main() -> None:
    """根据 mode 执行且仅执行一条数据管线路径。"""
    configure_console_encoding()
    args = parse_args()
    validate_args(args)
    end_time = None if args.end_time is None else pd.Timestamp(args.end_time)

    if args.mode == "eco":
        # 不连接 Wind，也不修改 DuckDB。
        print_result(build_eco_calendar_files())
        return

    if args.mode == "audit":
        # 只读审计，可用于运行前后核对数据覆盖范围。
        print_result(audit_database())
        print(f"CSV: {CSV_DATA_DIR}")
        print(f"Backups: {DUCKDB_BACKUP_DIR}")
        print(f"Logs: {DATA_PIPELINE_LOG_DIR}")
        return

    if args.mode == "historical-patch":
        # 数据来自 T_mindf.csv，不需要连接 Wind。
        print_result(
            run_historical_minute_patch(args.historical_patch_date)
        )
        return

    # 以下三种模式需要 Wind，统一在这里延迟连接。
    wind = start_wind()
    if args.mode == "full":
        # 首次流程：原始 Excel/T_mindf/Wind -> CSV -> 备份 -> DuckDB。
        result = run_full_initialization(
            wind,
            start_date=args.start_date,
            end_time=end_time,
            historical_patch_dates=(args.historical_patch_date,),
            pause_seconds=args.pause_seconds,
        )
    elif args.mode == "incremental":
        # 日常流程只更新尚未结束的合约，并对近期数据做重叠覆盖。
        result = run_incremental_update(
            wind,
            start_date=args.start_date,
            end_time=end_time,
            daily_overlap_trading_days=args.daily_overlap,
            minute_overlap_trading_days=args.minute_overlap,
            pause_seconds=args.pause_seconds,
        )
    else:
        # Wind 强制补丁仍遵循“先 CSV、再备份、后 DuckDB”的顺序。
        result = run_contract_wind_patch(
            wind,
            args.dataset,
            args.contract,
            args.patch_start,
            args.patch_end,
            pause_seconds=args.pause_seconds,
        )
    print_result(result)


if __name__ == "__main__":
    main()
