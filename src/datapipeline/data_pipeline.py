"""Wind-to-DuckDB treasury-futures data pipeline."""

from __future__ import annotations

import re
import time
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import duckdb
import pandas as pd

from .paths import DB_PATH, HISTORICAL_MINUTE_CSV

# ------------------------------ 用户配置 ------------------------------
# 数据覆盖区间与本地 DuckDB 文件位置。END_DATE 默认取运行当天。
START_DATE = "2015-06-30"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# Wind 品种代码，以及允许从 Wind 拉取分钟线的最早合约。
PRODUCT_WIND_CODE = "T.CFE"
MINUTE_WIND_START_CONTRACT = "T2503.CFE"
# CSV 的 total_turnover 以万元计，乘 10,000 后与 Wind amt 的元口径对齐。
CSV_TURNOVER_TO_WIND_AMT = 10_000.0

# 集中维护 Wind 请求字段，后续标准化函数会按这些字段生成固定列顺序。
DAILY_FIELDS = (
    "open", "high", "low", "close", "settle",
    "volume", "amt", "oi", "oi_chg",
)
MINUTE_WIND_FIELDS = (
    "open", "high", "low", "close", "volume",
    "amt", "oi",
)
# T 合约日盘交易时段；分钟请求会按上午、下午拆分，跳过午休和闭市时间。
MINUTE_TRADING_SESSIONS = (
    ("09:30:00", "11:30:00"),
    ("13:00:00", "15:15:00"),
)

DateLike = str | date | datetime | pd.Timestamp

# 数据库按“参照数据 -> 原始行情 -> 主力连续行情 -> 复权因子”分层建表。
# 所有原始表都设置主键，方便全量重跑和增量更新保持幂等。
SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS contracts (
    sec_name VARCHAR,
    code VARCHAR,
    wind_code VARCHAR PRIMARY KEY,
    delivery_month VARCHAR,
    change_limit DOUBLE,
    target_margin DOUBLE,
    contract_issue_date DATE,
    last_trade_date DATE,
    last_delivery_month DATE,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main_contract_mapping (
    trade_date DATE PRIMARY KEY,
    main_contract VARCHAR NOT NULL,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_bars (
    trade_date DATE NOT NULL,
    wind_code VARCHAR NOT NULL,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, settle DOUBLE,
    volume DOUBLE, amt DOUBLE, oi DOUBLE, oi_chg DOUBLE,
    ingested_at TIMESTAMP,
    PRIMARY KEY (wind_code, trade_date)
);

CREATE TABLE IF NOT EXISTS minute_bars (
    trade_date DATE NOT NULL,
    bar_time TIMESTAMP NOT NULL,
    wind_code VARCHAR NOT NULL,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume DOUBLE, amt DOUBLE, oi DOUBLE,
    begin_time TIMESTAMP, end_time TIMESTAMP,
    data_source VARCHAR DEFAULT 'wind',
    ingested_at TIMESTAMP,
    PRIMARY KEY (wind_code, bar_time)
);

CREATE TABLE IF NOT EXISTS main_daily_continuous (
    trade_date DATE PRIMARY KEY,
    main_contract VARCHAR NOT NULL,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, settle DOUBLE,
    volume DOUBLE, amt DOUBLE, oi DOUBLE, oi_chg DOUBLE,
    built_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main_minute_continuous (
    trade_date DATE NOT NULL,
    bar_time TIMESTAMP PRIMARY KEY,
    main_contract VARCHAR NOT NULL,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume DOUBLE, amt DOUBLE, oi DOUBLE,
    begin_time TIMESTAMP, end_time TIMESTAMP,
    data_source VARCHAR DEFAULT 'wind',
    built_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS main_adj_factor_list (
    segment_start DATE PRIMARY KEY,
    segment_end DATE NOT NULL,
    main_contract VARCHAR NOT NULL,
    next_contract VARCHAR,
    roll_date DATE,
    settle_reference_date DATE,
    old_settle DOUBLE,
    new_settle DOUBLE,
    roll_gap DOUBLE,
    roll_ratio DOUBLE,
    backward_adj_factor DOUBLE NOT NULL,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS eco_calendar (
    event_date DATE NOT NULL,
    event_time VARCHAR NOT NULL,
    event_datetime TIMESTAMP NOT NULL,
    region VARCHAR NOT NULL,
    indicator VARCHAR NOT NULL,
    importance VARCHAR,
    prev DOUBLE,
    forecast DOUBLE,
    actual DOUBLE,
    tf_category VARCHAR NOT NULL,
    ingested_at TIMESTAMP,
    PRIMARY KEY (event_datetime, region, indicator)
);

CREATE OR REPLACE VIEW main_contract_rolls AS
SELECT trade_date AS roll_date, main_contract
FROM (
    SELECT trade_date, main_contract,
           lag(main_contract) OVER (ORDER BY trade_date) AS previous_contract
    FROM main_contract_mapping
)
WHERE previous_contract IS NULL OR previous_contract <> main_contract;
"""

CONTRACT_COLUMNS = [
    "sec_name", "code", "wind_code", "delivery_month",
    "change_limit", "target_margin", "contract_issue_date",
    "last_trade_date", "last_delivery_month", "updated_at",
]
MAPPING_COLUMNS = ["trade_date", "main_contract", "updated_at"]
DAILY_COLUMNS = [
    "trade_date", "wind_code", *DAILY_FIELDS, "ingested_at",
]
MINUTE_COLUMNS = [
    "trade_date", "bar_time", "wind_code",
    "open", "high", "low", "close", "volume", "amt",
    "oi", "begin_time", "end_time", "data_source", "ingested_at",
]
FACTOR_COLUMNS = [
    "segment_start", "segment_end", "main_contract",
    "next_contract", "roll_date", "settle_reference_date",
    "old_settle", "new_settle", "roll_gap", "roll_ratio",
    "backward_adj_factor", "created_at",
]


# 复权视图只在查询时乘因子，不修改主力连续表中的原始价格。
MINUTE_ADJUSTED_VIEW_SQL = r"""
CREATE OR REPLACE VIEW v_main_minute_backward_adjusted AS
SELECT c.trade_date, c.bar_time, c.main_contract, f.backward_adj_factor,
       c.open * f.backward_adj_factor AS open,
       c.high * f.backward_adj_factor AS high,
       c.low * f.backward_adj_factor AS low,
       c.close * f.backward_adj_factor AS close,
       c.volume, c.amt, c.oi, c.begin_time, c.end_time, c.data_source
FROM main_minute_continuous c
JOIN main_adj_factor_list f
  ON c.main_contract = f.main_contract
 AND c.trade_date BETWEEN f.segment_start AND f.segment_end
"""


def open_database(path: str | Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """打开 DuckDB，并自动迁移旧版分钟时间字段。"""
    con = duckdb.connect(str(path))
    con.execute(SCHEMA_SQL)

    # 兼容旧数据库：先检查原始分钟表的列，再只补缺失或旧类型字段。
    minute_schema = {
        row[1]: row[2].upper()
        for row in con.execute("PRAGMA table_info('minute_bars')").fetchall()
    }
    if "data_source" not in minute_schema:
        con.execute(
            "ALTER TABLE minute_bars ADD COLUMN data_source VARCHAR DEFAULT 'wind'"
        )
    if minute_schema.get("begin_time") != "TIMESTAMP":
        con.execute(
            "ALTER TABLE minute_bars ALTER COLUMN begin_time TYPE TIMESTAMP "
            "USING bar_time"
        )
    if minute_schema.get("end_time") != "TIMESTAMP":
        con.execute(
            "ALTER TABLE minute_bars ALTER COLUMN end_time TYPE TIMESTAMP "
            "USING bar_time + INTERVAL '59 seconds'"
        )

    # 主力连续分钟表执行同样的兼容迁移。
    continuous_schema = {
        row[1]: row[2].upper()
        for row in con.execute(
            "PRAGMA table_info('main_minute_continuous')"
        ).fetchall()
    }
    needs_continuous_migration = (
        "data_source" not in continuous_schema
        or continuous_schema.get("begin_time") != "TIMESTAMP"
        or continuous_schema.get("end_time") != "TIMESTAMP"
    )
    if needs_continuous_migration:
        # DuckDB 修改被视图引用的表之前，需先移除依赖视图。
        con.execute("DROP VIEW IF EXISTS v_main_minute_backward_adjusted")
    if "data_source" not in continuous_schema:
        con.execute(
            "ALTER TABLE main_minute_continuous "
            "ADD COLUMN data_source VARCHAR DEFAULT 'wind'"
        )
    if continuous_schema.get("begin_time") != "TIMESTAMP":
        con.execute(
            "ALTER TABLE main_minute_continuous ALTER COLUMN begin_time "
            "TYPE TIMESTAMP USING bar_time"
        )
    if continuous_schema.get("end_time") != "TIMESTAMP":
        con.execute(
            "ALTER TABLE main_minute_continuous ALTER COLUMN end_time "
            "TYPE TIMESTAMP USING bar_time + INTERVAL '59 seconds'"
        )
    # 无论是否发生迁移，都重建视图，确保它引用最新表结构。
    con.execute(MINUTE_ADJUSTED_VIEW_SQL)
    return con


def _as_date(value: DateLike) -> date:
    """把字符串、datetime 或 Timestamp 统一为 Python date。"""
    return pd.Timestamp(value).date()


def _day_start(value: DateLike) -> pd.Timestamp:
    """返回给定日期的 00:00:00，用作分钟查询左边界。"""
    return pd.Timestamp(value).normalize()


def _day_end(value: DateLike) -> pd.Timestamp:
    """返回给定日期的 23:59:59，用作分钟查询右边界。"""
    return pd.Timestamp(value).normalize() + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)


def _upsert_frame(
    con: duckdb.DuckDBPyConnection,
    table: str,
    frame: pd.DataFrame,
    columns: Sequence[str],
) -> int:
    """按目标表主键幂等写入 DataFrame。"""
    if frame.empty:
        return 0
    # DuckDB 可直接扫描注册的 DataFrame；finally 保证临时注册名始终释放。
    batch = frame.loc[:, list(columns)].copy()
    column_sql = ", ".join(columns)
    con.register("_batch", batch)
    try:
        con.execute(
            f"INSERT OR REPLACE INTO {table} ({column_sql}) "
            f"SELECT {column_sql} FROM _batch"
        )
    finally:
        con.unregister("_batch")
    return len(batch)


def _insert_ignore_frame(
    con: duckdb.DuckDBPyConnection,
    table: str,
    frame: pd.DataFrame,
    columns: Sequence[str],
) -> int:
    """写入尚不存在的主键；用于避免历史 CSV 覆盖 Wind 数据。"""
    if frame.empty:
        return 0
    # INSERT OR IGNORE 让已存在的 Wind 行优先于后导入的历史 CSV 行。
    batch = frame.loc[:, list(columns)].copy()
    column_sql = ", ".join(columns)
    con.register("_batch_ignore", batch)
    try:
        con.execute(
            f"INSERT OR IGNORE INTO {table} ({column_sql}) "
            f"SELECT {column_sql} FROM _batch_ignore"
        )
    finally:
        con.unregister("_batch_ignore")
    return len(batch)


def _replace_frame_range(
    con: duckdb.DuckDBPyConnection,
    table: str,
    frame: pd.DataFrame,
    columns: Sequence[str],
    delete_sql: str,
    delete_params: Sequence[Any],
) -> int:
    """抓取成功后，在单一事务内替换一个区间；空结果不会删除旧数据。"""
    if frame.empty:
        return 0
    # 删除与写入放在同一事务中，任何异常都会恢复原区间数据。
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(delete_sql, list(delete_params))
        rows = _upsert_frame(con, table, frame, columns)
        con.execute("COMMIT")
        return rows
    except Exception:
        con.execute("ROLLBACK")
        raise

def _wind_frame(result: Any, context: str) -> pd.DataFrame:
    """解析 WindPy usedf=True 的 `(error_code, DataFrame)` 返回值。"""
    if isinstance(result, pd.DataFrame):
        return result.copy()
    if not isinstance(result, (tuple, list)) or len(result) < 2:
        raise RuntimeError(f"{context}: 无法识别 Wind 返回值 {type(result)!r}")
    error_code, frame = result[0], result[1]
    if error_code not in (0, None):
        raise RuntimeError(f"{context}: Wind error_code={error_code}")
    if not isinstance(frame, pd.DataFrame):
        raise RuntimeError(f"{context}: Wind 未返回 DataFrame")
    return frame.copy()


def _wind_call(callable_: Any, context: str, retries: int = 3) -> pd.DataFrame:
    """执行一次 Wind 调用；失败时按 1、2、4 秒指数退避重试。"""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return _wind_frame(callable_(), context)
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            # 指数退避可减少短时网络或 Wind 终端繁忙造成的连续失败。
            time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"{context}: 重试 {retries} 次仍失败") from last_error


def _normalise_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """复制 DataFrame，并把列名统一为去空格的小写形式。"""
    out = frame.copy()
    out.columns = [str(column).strip().lower() for column in out.columns]
    return out


def _normalise_contracts(raw: pd.DataFrame) -> pd.DataFrame:
    """把 futurecc 返回值整理成 contracts 表的固定字段和类型。"""
    frame = _normalise_columns(raw).reset_index(drop=True)
    for column in CONTRACT_COLUMNS[:-1]:
        if column not in frame.columns:
            frame[column] = None
    frame["wind_code"] = frame["wind_code"].astype("string").str.strip().str.upper()
    frame["code"] = frame["code"].astype("string").str.strip().str.upper()
    frame["delivery_month"] = frame["delivery_month"].astype("string")
    for column in ("change_limit", "target_margin"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("contract_issue_date", "last_trade_date", "last_delivery_month"):
        frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
    frame["updated_at"] = pd.Timestamp.now()
    frame = frame.dropna(subset=["wind_code"])
    return frame.drop_duplicates("wind_code", keep="last")[CONTRACT_COLUMNS]


def _normalise_mapping(raw: pd.DataFrame) -> pd.DataFrame:
    """把 trade_hiscode 时间序列整理成“交易日 -> 主力合约”映射。"""
    frame = _normalise_columns(raw)
    # 兼容少数 WindPy 版本未返回标准列名的情况，退回使用第一列。
    code_column = "trade_hiscode" if "trade_hiscode" in frame.columns else frame.columns[0]
    out = pd.DataFrame({
        "trade_date": pd.to_datetime(frame.index, errors="coerce"),
        "main_contract": frame[code_column].astype("string").str.strip().str.upper().to_numpy(),
    })
    out = out.dropna(subset=["trade_date", "main_contract"])
    out = out[out["main_contract"].str.endswith(".CFE", na=False)]
    out["trade_date"] = out["trade_date"].dt.date
    out["updated_at"] = pd.Timestamp.now()
    return out.drop_duplicates("trade_date", keep="last").sort_values("trade_date")


def fetch_contract_catalog(
    wind: Any, start_date: DateLike, end_date: DateLike, retries: int = 3
) -> pd.DataFrame:
    """从 Wind futurecc 获取区间内出现过的 T 合约目录。"""
    start, end = _as_date(start_date), _as_date(end_date)
    options = f"startdate={start:%Y-%m-%d};enddate={end:%Y-%m-%d};wind_code={PRODUCT_WIND_CODE}"
    raw = _wind_call(
        lambda: wind.wset("futurecc", options, usedf=True),
        f"wset futurecc {start}~{end}",
        retries,
    )
    return _normalise_contracts(raw)


def fetch_main_contract_mapping(
    wind: Any, start_date: DateLike, end_date: DateLike, retries: int = 3
) -> pd.DataFrame:
    """从 T.CFE 的 trade_hiscode 字段获取逐日主力合约代码。"""
    start, end = _as_date(start_date), _as_date(end_date)
    raw = _wind_call(
        lambda: wind.wsd(
            PRODUCT_WIND_CODE, "trade_hiscode",
            start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"),
            "", usedf=True,
        ),
        f"wsd trade_hiscode {start}~{end}",
        retries,
    )
    return _normalise_mapping(raw)


def refresh_reference_data(
    wind: Any,
    con: duckdb.DuckDBPyConnection,
    start_date: DateLike,
    end_date: DateLike,
    retries: int = 3,
) -> dict[str, int]:
    """刷新合约目录，并原子替换指定区间的逐日主力映射。"""
    start, end = _as_date(start_date), _as_date(end_date)
    contracts = fetch_contract_catalog(wind, start, end, retries)
    mapping = fetch_main_contract_mapping(wind, start, end, retries)
    contract_rows = _upsert_frame(con, "contracts", contracts, CONTRACT_COLUMNS)
    mapping_rows = _replace_frame_range(
        con, "main_contract_mapping", mapping, MAPPING_COLUMNS,
        "DELETE FROM main_contract_mapping WHERE trade_date BETWEEN ? AND ?",
        [start, end],
    )
    return {"contracts": contract_rows, "mapping_days": mapping_rows}


def incremental_refresh_reference_data(
    wind: Any,
    con: duckdb.DuckDBPyConnection,
    bootstrap_start_date: DateLike = START_DATE,
    end_date: DateLike = END_DATE,
    overlap_days: int = 10,
    retries: int = 3,
) -> dict[str, int]:
    """从已存映射末端向前重叠若干天刷新，吸收 Wind 的历史修订。"""
    max_date = con.execute("SELECT max(trade_date) FROM main_contract_mapping").fetchone()[0]
    bootstrap = _as_date(bootstrap_start_date)
    # overlap_days 避免只从最后一天之后续传而漏掉近期主力映射修订。
    start = bootstrap if max_date is None else max(bootstrap, max_date - timedelta(days=overlap_days))
    return refresh_reference_data(wind, con, start, end_date, retries)

def _normalise_daily(raw: pd.DataFrame, wind_code: str) -> pd.DataFrame:
    """把 Wind 日线返回值整理成 daily_bars 的固定列、类型与主键。"""
    frame = _normalise_columns(raw)
    out = pd.DataFrame({"trade_date": pd.to_datetime(frame.index, errors="coerce")})
    out["wind_code"] = wind_code.upper()
    for column in DAILY_FIELDS:
        out[column] = pd.to_numeric(frame[column], errors="coerce").to_numpy() if column in frame else None
    out = out.dropna(subset=["trade_date"])
    # 至少有一个价格字段有效才保留，避免写入 Wind 返回的全空占位行。
    out = out[out[["open", "high", "low", "close", "settle"]].notna().any(axis=1)]
    out["trade_date"] = out["trade_date"].dt.date
    out["ingested_at"] = pd.Timestamp.now()
    return out.drop_duplicates(["wind_code", "trade_date"], keep="last")[DAILY_COLUMNS]


def _normalise_minute(raw: pd.DataFrame, wind_code: str) -> pd.DataFrame:
    """把 Wind 分钟线统一成 minute_bars 结构，并标记来源为 wind。"""
    frame = _normalise_columns(raw).rename(columns={
        "amount": "amt", "position": "oi",
    })
    bar_time = pd.to_datetime(frame.index, errors="coerce")
    out = pd.DataFrame({"bar_time": bar_time})
    out["trade_date"] = bar_time.date
    out["wind_code"] = wind_code.upper()
    for column in ("open", "high", "low", "close", "volume", "amt", "oi"):
        out[column] = pd.to_numeric(frame[column], errors="coerce").to_numpy() if column in frame else None
    out["begin_time"] = bar_time
    # Wind 时间戳视为一分钟 bar 的起点，因此结束时间补到该分钟第 59 秒。
    out["end_time"] = bar_time + pd.Timedelta(seconds=59)
    out = out.dropna(subset=["bar_time"])
    out = out[out[["open", "high", "low", "close"]].notna().any(axis=1)]
    out["data_source"] = "wind"
    out["ingested_at"] = pd.Timestamp.now()
    return out.drop_duplicates(["wind_code", "bar_time"], keep="last")[MINUTE_COLUMNS]


HISTORICAL_MINUTE_COLUMNS = (
    "underlying_symbol", "datetime", "trading_date", "dominant_id",
    "open", "close", "high", "low", "total_turnover",
    "volume", "open_interest",
)


def _normalise_historical_minute_chunk(raw: pd.DataFrame) -> pd.DataFrame:
    """校验并转换一块 T_mindf.csv 数据，供分块导入复用。"""
    missing = sorted(set(HISTORICAL_MINUTE_COLUMNS) - set(raw.columns))
    if missing:
        raise ValueError(f"T_mindf.csv 缺少列: {missing}")
    frame = raw.loc[raw["underlying_symbol"].astype(str).str.upper() == "T"].copy()
    bar_time = pd.to_datetime(frame["datetime"], errors="coerce")
    out = pd.DataFrame({
        "trade_date": pd.to_datetime(
            frame["trading_date"], errors="coerce"
        ).dt.date.to_numpy(),
        "bar_time": bar_time.to_numpy(),
        "wind_code": frame["dominant_id"].astype("string").str.strip().str.upper().to_numpy(),
    })
    for column in ("open", "high", "low", "close", "volume"):
        out[column] = pd.to_numeric(frame[column], errors="coerce").to_numpy()
    # 历史 CSV 与 Wind 的成交额单位不同，先换算成统一的元口径。
    out["amt"] = (
        pd.to_numeric(frame["total_turnover"], errors="coerce").to_numpy()
        * CSV_TURNOVER_TO_WIND_AMT
    )
    out["oi"] = pd.to_numeric(frame["open_interest"], errors="coerce").to_numpy()
    out["begin_time"] = bar_time.to_numpy()
    out["end_time"] = (bar_time + pd.Timedelta(seconds=59)).to_numpy()
    out["data_source"] = "historical_csv"
    out["ingested_at"] = pd.Timestamp.now()
    out = out.dropna(subset=["trade_date", "bar_time", "wind_code"])
    out = out[out[["open", "high", "low", "close"]].notna().any(axis=1)]
    return (
        out.drop_duplicates(["wind_code", "bar_time"], keep="last")
        .sort_values(["bar_time", "wind_code"])[MINUTE_COLUMNS]
    )


def import_historical_minute_csv(
    con: duckdb.DuckDBPyConnection,
    csv_path: str | Path = HISTORICAL_MINUTE_CSV,
    chunksize: int = 100_000,
    replace_existing_csv: bool = True,
) -> dict[str, Any]:
    """分块导入主力分钟 CSV；已有 Wind 主键优先，不会被 CSV 覆盖。"""
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    rows_read = 0
    valid_rows = 0
    # 整批 CSV 使用同一事务：任一分块失败时，不留下半成品。
    con.execute("BEGIN TRANSACTION")
    try:
        if replace_existing_csv:
            con.execute("DELETE FROM minute_bars WHERE data_source = 'historical_csv'")
        reader = pd.read_csv(
            path, usecols=list(HISTORICAL_MINUTE_COLUMNS), chunksize=chunksize
        )
        for chunk_number, chunk in enumerate(reader, start=1):
            rows_read += len(chunk)
            frame = _normalise_historical_minute_chunk(chunk)
            valid_rows += len(frame)
            _insert_ignore_frame(con, "minute_bars", frame, MINUTE_COLUMNS)
            print(
                f"[historical csv {chunk_number}] read={rows_read:,}, valid={valid_rows:,}"
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    stored_rows, start_time, end_time, contracts = con.execute(
        """
        SELECT count(*), min(bar_time), max(bar_time), count(DISTINCT wind_code)
        FROM minute_bars WHERE data_source = 'historical_csv'
        """
    ).fetchone()
    return {
        "csv_path": str(path.resolve()),
        "rows_read": rows_read,
        "valid_rows": valid_rows,
        "historical_csv_rows_in_database": stored_rows,
        "contracts": contracts,
        "start_time": start_time,
        "end_time": end_time,
    }


def patch_historical_minute_date_from_csv(
    con: duckdb.DuckDBPyConnection,
    patch_date: DateLike = "2024-11-21",
    csv_path: str | Path = HISTORICAL_MINUTE_CSV,
    chunksize: int = 100_000,
) -> dict[str, Any]:
    """用 T_mindf.csv 覆盖指定日期的同键 Wind 行，并重建当天主力连续分钟。"""
    target = _as_date(patch_date)
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    pieces: list[pd.DataFrame] = []
    reader = pd.read_csv(
        path, usecols=list(HISTORICAL_MINUTE_COLUMNS), chunksize=chunksize
    )
    for chunk in reader:
        chunk_dates = pd.to_datetime(chunk["trading_date"], errors="coerce").dt.date
        selected = chunk.loc[chunk_dates.eq(target)]
        if not selected.empty:
            pieces.append(_normalise_historical_minute_chunk(selected))
    if not pieces:
        raise ValueError(f"T_mindf.csv 不包含 {target}")
    frame = (
        pd.concat(pieces, ignore_index=True)
        .drop_duplicates(["wind_code", "bar_time"], keep="last")
        .sort_values(["bar_time", "wind_code"])
    )
    if frame["trade_date"].nunique() != 1:
        raise ValueError(f"补丁读取到了目标日以外的数据: {sorted(frame['trade_date'].unique())}")

    # 这是窄范围修复：仅覆盖目标日 CSV 中实际存在的 (wind_code, bar_time) 主键。
    con.execute("BEGIN TRANSACTION")
    try:
        raw_rows = _upsert_frame(con, "minute_bars", frame, MINUTE_COLUMNS)
        con.execute("DELETE FROM main_minute_continuous WHERE trade_date = ?", [target])
        main_patch = frame.rename(columns={"wind_code": "main_contract"}).copy()
        main_patch["built_at"] = pd.Timestamp.now()
        main_columns = [
            "trade_date", "bar_time", "main_contract", "open", "high", "low",
            "close", "volume", "amt", "oi", "begin_time", "end_time",
            "data_source", "built_at",
        ]
        _upsert_frame(con, "main_minute_continuous", main_patch, main_columns)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    audit = con.execute(
        """
        SELECT count(*) AS rows, min(bar_time) AS start_time, max(bar_time) AS end_time,
               count(DISTINCT main_contract) AS contracts, min(data_source) AS data_source
        FROM main_minute_continuous WHERE trade_date = ?
        """,
        [target],
    ).fetchone()
    return {
        "patch_date": target,
        "raw_rows_upserted": raw_rows,
        "continuous_rows": audit[0],
        "start_time": audit[1],
        "end_time": audit[2],
        "contracts": audit[3],
        "data_source": audit[4],
    }


def fetch_daily_bars(
    wind: Any, wind_code: str, start_date: DateLike, end_date: DateLike, retries: int = 3
) -> pd.DataFrame:
    """调用 wsd 获取单份合约的日线，并返回标准化 DataFrame。"""
    start, end = _as_date(start_date), _as_date(end_date)
    raw = _wind_call(
        lambda: wind.wsd(
            wind_code, ",".join(DAILY_FIELDS),
            start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"),
            "unit=1", usedf=True,
        ),
        f"wsd {wind_code} {start}~{end}",
        retries,
    )
    return _normalise_daily(raw, wind_code)


def fetch_minute_bars(
    wind: Any,
    wind_code: str,
    start_time: DateLike,
    end_time: DateLike,
    retries: int = 3,
) -> pd.DataFrame:
    """调用 wsi 获取单份合约的一个分钟区间，并标准化字段。"""
    start, end = pd.Timestamp(start_time), pd.Timestamp(end_time)
    raw = _wind_call(
        lambda: wind.wsi(
            wind_code, ",".join(MINUTE_WIND_FIELDS),
            start.strftime("%Y-%m-%d %H:%M:%S"),
            end.strftime("%Y-%m-%d %H:%M:%S"),
            "", usedf=True,
        ),
        f"wsi {wind_code} {start}~{end}",
        retries,
    )
    return _normalise_minute(raw, wind_code)


def _active_contracts(
    con: duckdb.DuckDBPyConnection,
    start_date: DateLike,
    end_date: DateLike,
    contract_codes: Sequence[str] | None = None,
) -> pd.DataFrame:
    """筛选生命周期与目标区间有交集的合约，可再按代码白名单过滤。"""
    start, end = _as_date(start_date), _as_date(end_date)
    frame = con.execute(
        """
        SELECT wind_code, contract_issue_date, last_trade_date
        FROM contracts
        WHERE coalesce(contract_issue_date, DATE '1900-01-01') <= ?
          AND coalesce(last_trade_date, DATE '2999-12-31') >= ?
        ORDER BY contract_issue_date, wind_code
        """,
        [end, start],
    ).fetchdf()
    if contract_codes is not None:
        wanted = {code.upper() for code in contract_codes}
        frame = frame[frame["wind_code"].isin(wanted)]
    return frame.reset_index(drop=True)


def _contract_serial(wind_code: str) -> int:
    """从 T2503.CFE 提取可比较的数字序号 2503。"""
    match = re.fullmatch(r"T(\d{4})\.CFE", str(wind_code).upper())
    if match is None:
        raise ValueError(f"无法解析 T 合约代码: {wind_code}")
    return int(match.group(1))


def _filter_minute_wind_contracts(
    contracts: pd.DataFrame,
    minimum_contract: str = MINUTE_WIND_START_CONTRACT,
) -> pd.DataFrame:
    """Wind 分钟请求只保留 T2503.CFE 及之后的合约。"""
    threshold = _contract_serial(minimum_contract)
    keep = contracts["wind_code"].map(_contract_serial).ge(threshold)
    return contracts.loc[keep].reset_index(drop=True)


def _contract_dates(row: pd.Series, start: date, end: date) -> tuple[date, date]:
    """把用户请求区间裁剪到当前合约的上市日至最后交易日。"""
    issue = _as_date(row["contract_issue_date"]) if pd.notna(row["contract_issue_date"]) else start
    last = _as_date(row["last_trade_date"]) if pd.notna(row["last_trade_date"]) else end
    return max(start, issue), min(end, last)


def _summary_row(code: str, start: Any, end: Any, rows: int, status: str, error: str = "") -> dict[str, Any]:
    """生成统一的单合约更新摘要，便于批量任务最终汇总。"""
    return {"wind_code": code, "start": start, "end": end, "rows": rows, "status": status, "error": error}


def full_update_daily(
    wind: Any,
    con: duckdb.DuckDBPyConnection,
    start_date: DateLike,
    end_date: DateLike,
    contract_codes: Sequence[str] | None = None,
    refresh_reference: bool = True,
    retries: int = 3,
    continue_on_error: bool = False,
) -> pd.DataFrame:
    """全量重抓并替换每份合约在指定日期区间内的日线。"""
    start, end = _as_date(start_date), _as_date(end_date)
    # 先刷新合约生命周期和主力映射，后续才能确定需要抓取哪些合约。
    if refresh_reference:
        refresh_reference_data(wind, con, start, end, retries)
    contracts = _active_contracts(con, start, end, contract_codes)
    summary: list[dict[str, Any]] = []
    for number, row in contracts.iterrows():
        code = row["wind_code"]
        query_start, query_end = _contract_dates(row, start, end)
        print(f"[daily {number + 1}/{len(contracts)}] {code}: {query_start} ~ {query_end}")
        try:
            frame = fetch_daily_bars(wind, code, query_start, query_end, retries)
            rows = _replace_frame_range(
                con, "daily_bars", frame, DAILY_COLUMNS,
                "DELETE FROM daily_bars WHERE wind_code = ? AND trade_date BETWEEN ? AND ?",
                [code, query_start, query_end],
            )
            summary.append(_summary_row(code, query_start, query_end, rows, "ok" if rows else "no_data"))
        except Exception as exc:
            summary.append(_summary_row(code, query_start, query_end, 0, "error", str(exc)))
            if not continue_on_error:
                raise
    return pd.DataFrame(summary)


def incremental_update_daily(
    wind: Any,
    con: duckdb.DuckDBPyConnection,
    bootstrap_start_date: DateLike = START_DATE,
    end_date: DateLike = END_DATE,
    contract_codes: Sequence[str] | None = None,
    refresh_reference: bool = True,
    retries: int = 3,
    continue_on_error: bool = False,
) -> pd.DataFrame:
    """逐合约从数据库最大 trade_date 的下一天开始更新日线。"""
    bootstrap, end = _as_date(bootstrap_start_date), _as_date(end_date)
    if refresh_reference:
        incremental_refresh_reference_data(wind, con, bootstrap, end, retries=retries)
    contracts = _active_contracts(con, bootstrap, end, contract_codes)
    summary: list[dict[str, Any]] = []
    for number, row in contracts.iterrows():
        code = row["wind_code"]
        max_date = con.execute("SELECT max(trade_date) FROM daily_bars WHERE wind_code = ?", [code]).fetchone()[0]
        active_start, active_end = _contract_dates(row, bootstrap, end)
        # 已有数据从下一天续传；新合约则从其有效区间起点开始。
        query_start = active_start if max_date is None else max(active_start, max_date + timedelta(days=1))
        if query_start > active_end:
            summary.append(_summary_row(code, query_start, active_end, 0, "up_to_date"))
            continue
        print(f"[daily+ {number + 1}/{len(contracts)}] {code}: {query_start} ~ {active_end}")
        try:
            frame = fetch_daily_bars(wind, code, query_start, active_end, retries)
            rows = _upsert_frame(con, "daily_bars", frame, DAILY_COLUMNS)
            summary.append(_summary_row(code, query_start, active_end, rows, "ok" if rows else "no_data"))
        except Exception as exc:
            summary.append(_summary_row(code, query_start, active_end, 0, "error", str(exc)))
            if not continue_on_error:
                raise
    return pd.DataFrame(summary)


def _minute_query_days(
    con: duckdb.DuckDBPyConnection, start: pd.Timestamp, end: pd.Timestamp
) -> list[pd.Timestamp]:
    """优先复用逐日主力映射作为中金所交易日历；映射末日之后用工作日补齐。"""
    stored = con.execute(
        "SELECT trade_date FROM main_contract_mapping WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        [start.date(), end.date()],
    ).fetchdf()
    days = {pd.Timestamp(value) for value in stored.get("trade_date", pd.Series(dtype="datetime64[ns]"))}
    # 映射覆盖区间之后可能尚未同步交易日历，临时用工作日补齐候选日期。
    fallback_start = start.normalize() if not days else max(days) + pd.Timedelta(days=1)
    days.update(pd.bdate_range(fallback_start, end.normalize()))
    return sorted(day for day in days if start.normalize() <= day <= end.normalize())


def _session_timestamp(day: pd.Timestamp, clock: str) -> pd.Timestamp:
    """把交易日和 HH:MM:SS 字符串合成为完整时间戳。"""
    return pd.Timestamp(f"{day.date()} {clock}")


def _minute_session_chunks(
    day: pd.Timestamp, start: pd.Timestamp, end: pd.Timestamp
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """把单日请求裁剪为 T 合约上午、下午两个有效交易时段。"""
    chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for open_clock, close_clock in MINUTE_TRADING_SESSIONS:
        session_open = _session_timestamp(day, open_clock)
        session_close = _session_timestamp(day, close_clock)
        chunk_start = max(start, session_open)
        chunk_end = min(end, session_close)
        if chunk_start <= chunk_end:
            chunks.append((chunk_start, chunk_end))
    return chunks


def _latest_queryable_minute(
    con: duckdb.DuckDBPyConnection, end_time: DateLike
) -> pd.Timestamp | None:
    """把结束时间回退到最近一个真实交易时段内，避免请求闭市区间。"""
    end = pd.Timestamp(end_time)
    trade_date = con.execute(
        "SELECT max(trade_date) FROM main_contract_mapping WHERE trade_date <= ?",
        [end.date()],
    ).fetchone()[0]
    if trade_date is None:
        return None
    day = pd.Timestamp(trade_date)
    morning_open = _session_timestamp(day, MINUTE_TRADING_SESSIONS[0][0])
    morning_close = _session_timestamp(day, MINUTE_TRADING_SESSIONS[0][1])
    afternoon_open = _session_timestamp(day, MINUTE_TRADING_SESSIONS[1][0])
    afternoon_close = _session_timestamp(day, MINUTE_TRADING_SESSIONS[1][1])
    # 如果最近交易日在 end 之前，最多只能查到该交易日收盘。
    if trade_date < end.date():
        return afternoon_close
    # 当天开盘前请求时，回退到上一交易日下午收盘。
    if end < morning_open:
        previous_date = con.execute(
            "SELECT max(trade_date) FROM main_contract_mapping WHERE trade_date < ?",
            [trade_date],
        ).fetchone()[0]
        if previous_date is None:
            return None
        return _session_timestamp(
            pd.Timestamp(previous_date), MINUTE_TRADING_SESSIONS[-1][1]
        )
    if end <= morning_close:
        return end
    # 午休期间回退到上午收盘；收盘后则回退到下午收盘。
    if end < afternoon_open:
        return morning_close
    if end <= afternoon_close:
        return end
    return afternoon_close


def fetch_minute_range(
    wind: Any,
    con: duckdb.DuckDBPyConnection,
    wind_code: str,
    start_time: DateLike,
    end_time: DateLike,
    retries: int = 3,
    pause_seconds: float = 0.0,
) -> pd.DataFrame:
    """按交易日、按上午/下午交易时段调用 wsi。"""
    start, end = pd.Timestamp(start_time), pd.Timestamp(end_time)
    # 逐日、逐交易时段的小请求更不容易触发 Wind 单次数据量限制。
    pieces: list[pd.DataFrame] = []
    for day in _minute_query_days(con, start, end):
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
    return pd.concat(pieces, ignore_index=True).drop_duplicates(["wind_code", "bar_time"], keep="last")


def full_update_minute(
    wind: Any,
    con: duckdb.DuckDBPyConnection,
    start_date: DateLike,
    end_date: DateLike,
    contract_codes: Sequence[str] | None = None,
    minimum_contract: str = MINUTE_WIND_START_CONTRACT,
    refresh_reference: bool = False,
    retries: int = 3,
    pause_seconds: float = 0.0,
    continue_on_error: bool = False,
) -> pd.DataFrame:
    """全量重抓并替换每份合约在指定日期区间内的分钟线。"""
    start_date_value, end_date_value = _as_date(start_date), _as_date(end_date)
    if refresh_reference:
        refresh_reference_data(wind, con, start_date_value, end_date_value, retries)
    contracts = _active_contracts(con, start_date_value, end_date_value, contract_codes)
    # T2503 以前的分钟历史由 CSV 提供，不再向 Wind 重复请求。
    contracts = _filter_minute_wind_contracts(contracts, minimum_contract)
    summary: list[dict[str, Any]] = []
    for number, row in contracts.iterrows():
        active_start, active_end = _contract_dates(row, start_date_value, end_date_value)
        query_start, query_end = _day_start(active_start), _day_end(active_end)
        code = row["wind_code"]
        print(f"[minute {number + 1}/{len(contracts)}] {code}: {query_start} ~ {query_end}")
        try:
            frame = fetch_minute_range(wind, con, code, query_start, query_end, retries, pause_seconds)
            rows = _replace_frame_range(
                con, "minute_bars", frame, MINUTE_COLUMNS,
                "DELETE FROM minute_bars WHERE wind_code = ? AND bar_time BETWEEN ? AND ?",
                [code, query_start, query_end],
            )
            summary.append(_summary_row(code, query_start, query_end, rows, "ok" if rows else "no_data"))
        except Exception as exc:
            summary.append(_summary_row(code, query_start, query_end, 0, "error", str(exc)))
            if not continue_on_error:
                raise
    return pd.DataFrame(summary)


def incremental_update_minute(
    wind: Any,
    con: duckdb.DuckDBPyConnection,
    bootstrap_start_date: DateLike = START_DATE,
    end_time: DateLike | None = None,
    contract_codes: Sequence[str] | None = None,
    minimum_contract: str = MINUTE_WIND_START_CONTRACT,
    refresh_reference: bool = False,
    retries: int = 3,
    pause_seconds: float = 0.0,
    continue_on_error: bool = False,
) -> pd.DataFrame:
    """逐合约从数据库最大 bar_time 的下一分钟开始更新。"""
    bootstrap = _as_date(bootstrap_start_date)
    end = pd.Timestamp.now().floor("s") if end_time is None else pd.Timestamp(end_time)
    if refresh_reference:
        incremental_refresh_reference_data(wind, con, bootstrap, end.date(), retries=retries)
    contracts = _active_contracts(con, bootstrap, end.date(), contract_codes)
    contracts = _filter_minute_wind_contracts(contracts, minimum_contract)
    summary: list[dict[str, Any]] = []
    for number, row in contracts.iterrows():
        code = row["wind_code"]
        max_time = con.execute(
            "SELECT max(bar_time) FROM minute_bars WHERE wind_code = ? AND data_source = 'wind'",
            [code],
        ).fetchone()[0]
        active_start_date, active_end_date = _contract_dates(row, bootstrap, end.date())
        active_start = _day_start(active_start_date)
        requested_end = min(_day_end(active_end_date), end)
        # 将当前时刻裁剪到最近的实际交易分钟，避免对午休或闭市区间发请求。
        active_end = _latest_queryable_minute(con, requested_end)
        # 只参考 Wind 来源的最大时间；历史 CSV 不应阻止 Wind 后续覆盖同一时期。
        query_start = active_start if max_time is None else max(active_start, pd.Timestamp(max_time) + pd.Timedelta(minutes=1))
        if active_end is None or query_start > active_end:
            summary.append(_summary_row(code, query_start, active_end, 0, "up_to_date"))
            continue
        print(f"[minute+ {number + 1}/{len(contracts)}] {code}: {query_start} ~ {active_end}")
        try:
            frame = fetch_minute_range(wind, con, code, query_start, active_end, retries, pause_seconds)
            rows = _upsert_frame(con, "minute_bars", frame, MINUTE_COLUMNS)
            summary.append(_summary_row(code, query_start, active_end, rows, "ok" if rows else "no_data"))
        except Exception as exc:
            summary.append(_summary_row(code, query_start, active_end, 0, "error", str(exc)))
            if not continue_on_error:
                raise
    return pd.DataFrame(summary)

def _mapping_bounds(
    con: duckdb.DuckDBPyConnection, start_date: DateLike | None, end_date: DateLike | None
) -> tuple[date, date]:
    """确定连续合约重建区间；未传边界时使用主力映射的完整范围。"""
    minimum, maximum = con.execute("SELECT min(trade_date), max(trade_date) FROM main_contract_mapping").fetchone()
    if minimum is None or maximum is None:
        raise ValueError("main_contract_mapping 为空，请先刷新主力映射")
    return (_as_date(start_date) if start_date is not None else minimum, _as_date(end_date) if end_date is not None else maximum)


def build_main_daily_continuous(
    con: duckdb.DuckDBPyConnection,
    start_date: DateLike | None = None,
    end_date: DateLike | None = None,
) -> dict[str, int]:
    """按逐日主力映射，从原始日线重建未复权主力连续日线。"""
    start, end = _mapping_bounds(con, start_date, end_date)
    # 删除和重建放在同一事务，失败时保留重建前的数据。
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM main_daily_continuous WHERE trade_date BETWEEN ? AND ?", [start, end])
        con.execute(
            """
            INSERT INTO main_daily_continuous
            SELECT d.trade_date, m.main_contract, d.open, d.high, d.low, d.close, d.settle,
                   d.volume, d.amt, d.oi, d.oi_chg, current_timestamp
            FROM main_contract_mapping AS m
            JOIN daily_bars AS d
              ON d.trade_date = m.trade_date AND d.wind_code = m.main_contract
            WHERE m.trade_date BETWEEN ? AND ?
            ORDER BY d.trade_date
            """,
            [start, end],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    # 同时报告缺失天数，便于发现“有主力映射但缺少对应日线”的数据洞。
    built = con.execute("SELECT count(*) FROM main_daily_continuous WHERE trade_date BETWEEN ? AND ?", [start, end]).fetchone()[0]
    missing = con.execute(
        """
        SELECT count(*) FROM main_contract_mapping m
        LEFT JOIN daily_bars d ON d.trade_date = m.trade_date AND d.wind_code = m.main_contract
        WHERE m.trade_date BETWEEN ? AND ? AND d.wind_code IS NULL
        """,
        [start, end],
    ).fetchone()[0]
    return {"built_rows": built, "missing_mapping_days": missing}


def build_main_minute_continuous(
    con: duckdb.DuckDBPyConnection,
    start_date: DateLike | None = None,
    end_date: DateLike | None = None,
) -> dict[str, int]:
    """合并 Wind 与历史 CSV，重建未复权主力连续分钟线。"""
    start, end = _mapping_bounds(con, start_date, end_date)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM main_minute_continuous WHERE trade_date BETWEEN ? AND ?", [start, end])
        # candidates 同时收集 Wind 当日主力和历史 CSV；数字越小优先级越高。
        # 同一 bar_time 若两种来源都存在，row_number 最终只保留 Wind。
        con.execute(
            """
            INSERT INTO main_minute_continuous (
                trade_date, bar_time, main_contract, open, high, low, close,
                volume, amt, oi, begin_time, end_time, data_source, built_at
            )
            WITH candidates AS (
                SELECT b.trade_date, b.bar_time, m.main_contract,
                       b.open, b.high, b.low, b.close, b.volume, b.amt, b.oi,
                       b.begin_time, b.end_time, 'wind' AS data_source, 1 AS source_priority
                FROM main_contract_mapping AS m
                JOIN minute_bars AS b
                  ON b.trade_date = m.trade_date AND b.wind_code = m.main_contract
                WHERE m.trade_date BETWEEN ? AND ?
                  AND coalesce(b.data_source, 'wind') = 'wind'

                UNION ALL

                SELECT b.trade_date, b.bar_time, b.wind_code AS main_contract,
                       b.open, b.high, b.low, b.close, b.volume, b.amt, b.oi,
                       b.begin_time, b.end_time, 'historical_csv' AS data_source,
                       2 AS source_priority
                FROM minute_bars AS b
                WHERE b.trade_date BETWEEN ? AND ?
                  AND b.data_source = 'historical_csv'
            ),
            ranked AS (
                SELECT *, row_number() OVER (
                    PARTITION BY bar_time ORDER BY source_priority
                ) AS source_rank
                FROM candidates
            )
            SELECT trade_date, bar_time, main_contract, open, high, low, close,
                   volume, amt, oi, begin_time, end_time, data_source, current_timestamp
            FROM ranked
            WHERE source_rank = 1
            ORDER BY bar_time
            """,
            [start, end, start, end],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    built = con.execute("SELECT count(*) FROM main_minute_continuous WHERE trade_date BETWEEN ? AND ?", [start, end]).fetchone()[0]
    mapped_days = con.execute("SELECT count(DISTINCT trade_date) FROM main_minute_continuous WHERE trade_date BETWEEN ? AND ?", [start, end]).fetchone()[0]
    return {"built_rows": built, "days_with_minute_data": mapped_days}


def build_main_continuous(
    con: duckdb.DuckDBPyConnection,
    start_date: DateLike | None = None,
    end_date: DateLike | None = None,
) -> dict[str, dict[str, int]]:
    """统一重建日线和分钟线主力连续表，并返回两部分统计。"""
    return {
        "daily": build_main_daily_continuous(con, start_date, end_date),
        "minute": build_main_minute_continuous(con, start_date, end_date),
    }

def _settle_pair_for_roll(
    con: duckdb.DuckDBPyConnection,
    old_contract: str,
    new_contract: str,
    roll_date: date,
    lookback_days: int,
) -> tuple[date, float, float] | None:
    """寻找换月日或其之前最近一次新旧合约都有效的结算价。"""
    prices = con.execute(
        """
        SELECT trade_date, wind_code, settle
        FROM daily_bars
        WHERE wind_code IN (?, ?)
          AND trade_date BETWEEN ? AND ?
          AND settle IS NOT NULL
        ORDER BY trade_date
        """,
        [old_contract, new_contract, roll_date - timedelta(days=lookback_days), roll_date],
    ).fetchdf()
    if prices.empty:
        return None
    # 透视后每行是一个交易日、两列分别是新旧合约，便于寻找共同非空日。
    pivot = prices.pivot_table(index="trade_date", columns="wind_code", values="settle", aggfunc="last")
    if old_contract not in pivot or new_contract not in pivot:
        return None
    common = pivot[[old_contract, new_contract]].dropna()
    if common.empty:
        return None
    # 查询已按日期升序排列，因此最后一行就是离换月日最近的共同交易日。
    reference_date = common.index[-1]
    return _as_date(reference_date), float(common.iloc[-1][old_contract]), float(common.iloc[-1][new_contract])


def create_adjusted_views(con: duckdb.DuckDBPyConnection) -> None:
    """创建便捷后复权视图；原始连续表保持未复权。"""
    con.execute(
        """
        CREATE OR REPLACE VIEW v_main_daily_backward_adjusted AS
        SELECT c.trade_date, c.main_contract, f.backward_adj_factor,
               c.open * f.backward_adj_factor AS open,
               c.high * f.backward_adj_factor AS high,
               c.low * f.backward_adj_factor AS low,
               c.close * f.backward_adj_factor AS close,
               c.settle * f.backward_adj_factor AS settle,
               c.volume, c.amt, c.oi, c.oi_chg
        FROM main_daily_continuous c
        JOIN main_adj_factor_list f
          ON c.main_contract = f.main_contract
         AND c.trade_date BETWEEN f.segment_start AND f.segment_end
        """
    )
    con.execute(MINUTE_ADJUSTED_VIEW_SQL)


def build_adj_factor_list(
    con: duckdb.DuckDBPyConnection,
    lookback_days: int = 15,
    strict: bool = True,
) -> pd.DataFrame:
    """按主力区间生成 settle-based 乘法后复权因子列表。"""
    mapping = con.execute(
        "SELECT trade_date, main_contract FROM main_contract_mapping ORDER BY trade_date"
    ).fetchdf()
    if mapping.empty:
        raise ValueError("main_contract_mapping 为空")
    # 主力代码发生变化时 segment_id 加 1，从而把映射压缩成连续的主力区间。
    mapping["segment_id"] = mapping["main_contract"].ne(mapping["main_contract"].shift()).cumsum()
    segments = (
        mapping.groupby("segment_id", as_index=False)
        .agg(
            segment_start=("trade_date", "min"),
            segment_end=("trade_date", "max"),
            main_contract=("main_contract", "first"),
        )
        .drop(columns="segment_id")
    )
    # 每段的下一合约及其起始日，就是当前段对应的换月信息。
    segments["next_contract"] = segments["main_contract"].shift(-1)
    segments["roll_date"] = segments["segment_start"].shift(-1)
    for column in ("settle_reference_date", "old_settle", "new_settle", "roll_gap", "roll_ratio"):
        segments[column] = None

    # 最后一段尚无下一合约，因此只计算前 len(segments)-1 次换月。
    for index in range(len(segments) - 1):
        old_contract = segments.at[index, "main_contract"]
        new_contract = segments.at[index, "next_contract"]
        roll_date = _as_date(segments.at[index, "roll_date"])
        pair = _settle_pair_for_roll(con, old_contract, new_contract, roll_date, lookback_days)
        if pair is None:
            message = f"{roll_date}: {old_contract} -> {new_contract} 找不到共同结算价"
            # strict 模式宁可中止，也不静默生成可能错误的复权序列。
            if strict:
                raise ValueError(message)
            warnings.warn(message + "；本次换月因子临时按 1 处理", stacklevel=2)
            reference_date, old_settle, new_settle = roll_date, 1.0, 1.0
        else:
            reference_date, old_settle, new_settle = pair
        if old_settle == 0:
            raise ZeroDivisionError(f"{reference_date} {old_contract} settle=0")
        segments.at[index, "settle_reference_date"] = reference_date
        segments.at[index, "old_settle"] = old_settle
        segments.at[index, "new_settle"] = new_settle
        segments.at[index, "roll_gap"] = new_settle - old_settle
        segments.at[index, "roll_ratio"] = new_settle / old_settle

    # 中文后复权口径固定最早一段为 1；每次换月后累乘 old/new。
    factors = [1.0] * len(segments)
    cumulative = 1.0
    for index in range(1, len(segments)):
        cumulative /= float(segments.at[index - 1, "roll_ratio"])
        factors[index] = cumulative
    segments["backward_adj_factor"] = factors
    segments["created_at"] = pd.Timestamp.now()
    for column in ("segment_start", "segment_end", "roll_date", "settle_reference_date"):
        segments[column] = pd.to_datetime(segments[column], errors="coerce").dt.date
    factors_frame = segments[FACTOR_COLUMNS]

    # 因子表整体重算并原子替换，避免新旧口径混存。
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM main_adj_factor_list")
        _upsert_frame(con, "main_adj_factor_list", factors_frame, FACTOR_COLUMNS)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    create_adjusted_views(con)
    return factors_frame
