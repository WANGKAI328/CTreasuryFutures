"""CSV-first 管线的公共文件 IO、日志和合约筛选工具。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
import json
import os
from pathlib import Path
import re
from typing import Any, Iterable, Literal, Sequence
from uuid import uuid4

import pandas as pd

from .data_pipeline import (
    DAILY_COLUMNS,
    MINUTE_COLUMNS,
    _as_date,
)
from .paths import DAILY_CSV_DIR, DATA_PIPELINE_LOG_DIR, MINUTE_CSV_DIR


MarketDataset = Literal["daily", "minute"]


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime, pd.Timestamp, Path)):
        return str(value)
    if pd.isna(value):
        return ""
    return str(value)


@dataclass
class PipelineRunLog:
    """每次运行创建独立目录，保存文本日志、JSONL 和摘要文件。"""

    run_type: str
    root: Path = DATA_PIPELINE_LOG_DIR
    run_id: str = field(init=False)
    run_dir: Path = field(init=False)
    log_path: Path = field(init=False)
    events_path: Path = field(init=False)
    started_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        stamp = self.started_at.strftime("%Y%m%d_%H%M%S")
        self.run_id = f"{stamp}_{self.run_type}_{uuid4().hex[:8]}"
        self.run_dir = Path(self.root) / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=False)
        self.log_path = self.run_dir / "pipeline.log"
        self.events_path = self.run_dir / "events.jsonl"
        self.event("run_started", run_type=self.run_type)

    def event(self, event: str, level: str = "INFO", **details: Any) -> None:
        timestamp = datetime.now().isoformat(timespec="seconds")
        payload = {
            "timestamp": timestamp,
            "level": level,
            "event": event,
            **details,
        }
        line = json.dumps(payload, ensure_ascii=False, default=_json_default)
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        readable = f"{timestamp} [{level}] {event}"
        if details:
            readable += " | " + json.dumps(
                details, ensure_ascii=False, default=_json_default
            )
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(readable + "\n")
        print(readable)

    def save_frame(self, name: str, frame: pd.DataFrame) -> Path:
        path = self.run_dir / f"{name}.csv"
        frame.to_csv(path, index=False, encoding="utf-8-sig")
        return path

    def finish(self, status: str, **summary: Any) -> Path:
        payload = {
            "run_id": self.run_id,
            "run_type": self.run_type,
            "status": status,
            "started_at": self.started_at.isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            **summary,
        }
        path = self.run_dir / "run_summary.json"
        _atomic_write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
            path,
        )
        self.event("run_finished", status=status, summary_path=str(path))
        return path


def _atomic_write_text(text: str, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_write_csv(
    frame: pd.DataFrame,
    path: str | Path,
    columns: Sequence[str],
) -> Path:
    """写临时文件并回读校验，成功后原子替换正式 CSV。"""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{target.name} 缺少待写入列: {missing}")
    clean = frame.loc[:, list(columns)].copy()
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        clean.to_csv(
            temporary,
            index=False,
            encoding="utf-8-sig",
            date_format="%Y-%m-%d %H:%M:%S",
        )
        check = pd.read_csv(temporary, encoding="utf-8-sig")
        if list(check.columns) != list(columns):
            raise ValueError(f"{target.name} 回读列顺序不一致")
        if len(check) != len(clean):
            raise ValueError(
                f"{target.name} 回读行数不一致: write={len(clean)}, read={len(check)}"
            )
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return target


def _read_csv(path: str | Path, dataset: str) -> pd.DataFrame:
    """按数据集类型恢复 CSV 的日期和时间字段。"""
    source = Path(path)
    if not source.is_file():
        return pd.DataFrame()
    frame = pd.read_csv(source, encoding="utf-8-sig")
    if dataset == "contracts":
        for column in (
            "contract_issue_date",
            "last_trade_date",
            "last_delivery_month",
        ):
            if column in frame:
                frame[column] = pd.to_datetime(frame[column], errors="coerce").dt.date
        if "updated_at" in frame:
            frame["updated_at"] = pd.to_datetime(
                frame["updated_at"], errors="coerce"
            )
    elif dataset == "mapping":
        frame["trade_date"] = pd.to_datetime(
            frame["trade_date"], errors="coerce"
        ).dt.date
        frame["updated_at"] = pd.to_datetime(
            frame["updated_at"], errors="coerce"
        )
    elif dataset == "daily":
        frame["trade_date"] = pd.to_datetime(
            frame["trade_date"], errors="coerce"
        ).dt.date
        frame["ingested_at"] = pd.to_datetime(
            frame["ingested_at"], errors="coerce"
        )
    elif dataset == "minute":
        frame["trade_date"] = pd.to_datetime(
            frame["trade_date"], errors="coerce"
        ).dt.date
        for column in ("bar_time", "begin_time", "end_time", "ingested_at"):
            frame[column] = pd.to_datetime(frame[column], errors="coerce")
    elif dataset == "eco":
        frame["date"] = pd.to_datetime(
            frame["date"], errors="coerce"
        ).dt.normalize()
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    else:
        raise ValueError(f"未知 CSV 数据集: {dataset}")
    return frame


def _market_csv_path(
    dataset: MarketDataset,
    wind_code: str,
    daily_dir: str | Path = DAILY_CSV_DIR,
    minute_dir: str | Path = MINUTE_CSV_DIR,
) -> Path:
    code = str(wind_code).strip().upper()
    if re.fullmatch(r"[A-Z0-9_.-]+", code) is None:
        raise ValueError(f"非法合约代码，不能作为 CSV 文件名: {wind_code!r}")
    base = Path(daily_dir if dataset == "daily" else minute_dir)
    return base / f"{code}.csv"


def _merge_reference_frame(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    keys: Sequence[str],
    columns: Sequence[str],
) -> pd.DataFrame:
    if existing.empty:
        combined = incoming.copy()
    elif incoming.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, incoming], ignore_index=True)
    return (
        combined.drop_duplicates(list(keys), keep="last")
        .sort_values(list(keys))
        .reset_index(drop=True)[list(columns)]
    )


def _contract_frame_for_update(
    contracts: pd.DataFrame,
    start: date,
    end: date,
    full: bool,
    contract_codes: Sequence[str] | None,
) -> pd.DataFrame:
    """全量选择区间内所有合约；增量只选择运行日尚未到期的合约。"""
    frame = contracts.copy()
    issue = pd.to_datetime(frame["contract_issue_date"], errors="coerce").dt.date
    last = pd.to_datetime(frame["last_trade_date"], errors="coerce").dt.date
    intersects = issue.fillna(date(1900, 1, 1)).le(end) & last.fillna(
        date(2999, 12, 31)
    ).ge(start)
    frame = frame.loc[intersects].copy()
    if not full:
        active_now = last.loc[frame.index].isna() | last.loc[frame.index].ge(end)
        frame = frame.loc[active_now]
    if contract_codes is not None:
        wanted = {str(code).strip().upper() for code in contract_codes}
        frame = frame.loc[frame["wind_code"].isin(wanted)]
    return frame.sort_values(
        ["contract_issue_date", "wind_code"]
    ).reset_index(drop=True)


def _overlap_date_from_existing(
    values: Iterable[Any], overlap_trading_days: int, fallback: date
) -> date:
    dates = sorted({_as_date(value) for value in values if pd.notna(value)})
    if not dates:
        return fallback
    index = max(0, len(dates) - max(1, overlap_trading_days))
    return max(fallback, dates[index])


def _replace_market_csv_range(
    dataset: MarketDataset,
    path: str | Path,
    incoming: pd.DataFrame,
    range_start: Any,
    range_end: Any,
) -> dict[str, int]:
    """用新数据替换合约 CSV 的指定区间，区间之外原样保留。"""
    if incoming.empty:
        return {"old_rows": len(_read_csv(path, dataset)), "stored_rows": 0}
    columns = DAILY_COLUMNS if dataset == "daily" else MINUTE_COLUMNS
    keys = (
        ["wind_code", "trade_date"]
        if dataset == "daily"
        else ["wind_code", "bar_time"]
    )
    timestamp_column = "trade_date" if dataset == "daily" else "bar_time"
    existing = _read_csv(path, dataset)
    old_rows = len(existing)
    if existing.empty:
        base = existing
    else:
        values = pd.to_datetime(existing[timestamp_column], errors="coerce")
        start, end = pd.Timestamp(range_start), pd.Timestamp(range_end)
        base = existing.loc[~values.between(start, end, inclusive="both")]
    combined = (
        pd.concat([base, incoming], ignore_index=True)
        .drop_duplicates(keys, keep="last")
        .sort_values(keys)
        .reset_index(drop=True)[columns]
    )
    _atomic_write_csv(combined, path, columns)
    return {"old_rows": old_rows, "stored_rows": len(combined)}


def _summary_row(
    dataset: str,
    wind_code: str,
    start: Any,
    end: Any,
    incoming_rows: int,
    stored_rows: int,
    status: str,
    csv_path: str | Path,
    error: str = "",
) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "wind_code": wind_code,
        "start": start,
        "end": end,
        "incoming_rows": incoming_rows,
        "stored_rows": stored_rows,
        "status": status,
        "csv_path": str(Path(csv_path).resolve()),
        "error": error,
    }


def _combine_summaries(*frames: pd.DataFrame) -> pd.DataFrame:
    usable = [frame for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return pd.DataFrame(
            columns=[
                "dataset",
                "wind_code",
                "start",
                "end",
                "incoming_rows",
                "stored_rows",
                "status",
                "csv_path",
                "error",
            ]
        )
    return pd.concat(usable, ignore_index=True)


def _raise_update_errors(summary: pd.DataFrame, label: str) -> None:
    if summary.empty or "status" not in summary:
        return
    errors = summary.loc[summary["status"].eq("error")]
    if errors.empty:
        return
    sample = errors[["wind_code", "start", "end", "error"]].head(10)
    raise RuntimeError(
        f"{label} 有 {len(errors)} 个合约失败:\n{sample.to_string(index=False)}"
    )
