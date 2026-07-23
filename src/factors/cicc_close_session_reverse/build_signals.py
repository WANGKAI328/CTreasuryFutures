"""构建 CICC 尾盘反转九类信号并输出 Excel。

从项目根目录直接运行：

    python src/factors/cicc_close_session_reverse/build_signals.py

安装过本项目后，也可以运行：

    python -m factors.cicc_close_session_reverse.build_signals
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

import numpy as np
import pandas as pd

# 允许用户不安装项目，直接从项目根目录运行本文件。
if __package__ in {None, ""}:
    SRC_DIR = Path(__file__).resolve().parents[2]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from factors.paths import FACTOR_LOG_DIR
    from factors.cicc_close_session_reverse.signal_builder import (
        CATALOG,
        SIGNAL_XLSX,
        add_oi_trend_event_filters,
        add_reversal_and_v2,
        add_semantic_triggers,
        assert_historical_thresholds,
        build_segment_features,
        load_inputs,
        load_roll_monitor,
        prepare_minute,
    )
else:
    from ..paths import FACTOR_LOG_DIR
    from .signal_builder import (
        CATALOG,
        SIGNAL_XLSX,
        add_oi_trend_event_filters,
        add_reversal_and_v2,
        add_semantic_triggers,
        assert_historical_thresholds,
        build_segment_features,
        load_inputs,
        load_roll_monitor,
        prepare_minute,
    )


@dataclass(frozen=True, slots=True)
class SignalBuildResult:
    """一次信号构建的关键结果。"""

    output_path: Path
    log_dir: Path
    signal_rows: int
    signal_dates: int
    factor_count: int
    blocked_signal_rows: int
    blocked_signal_dates: int
    missing_monitor_dates: int

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["output_path"] = str(self.output_path.resolve())
        result["log_dir"] = str(self.log_dir.resolve())
        return result


def _json_default(value: Any) -> str | int | float | bool | None:
    if isinstance(value, Path):
        return str(value.resolve())
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if pd.isna(value):
        return None
    return str(value)


def _atomic_write_json(payload: dict[str, Any], path: Path) -> None:
    """原子写入运行摘要，避免中途中断留下半个 JSON。"""
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
    logger = logging.getLogger(f"factor_signal_build.{run_dir.name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.FileHandler(
        run_dir / "build_signals.log",
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


def _build_signal_frame(
    features: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, int, int, int]:
    """把宽表触发器转换为信号明细，并合并每日移仓监控。"""
    signal_columns = CATALOG["factor_id"].tolist()
    missing_triggers = [
        column for column in signal_columns if column not in features.columns
    ]
    if missing_triggers:
        raise ValueError(f"缺少信号触发列: {missing_triggers}")

    signals = features.melt(
        id_vars=["trading_date"],
        value_vars=signal_columns,
        var_name="factor_id",
        value_name="signal",
    )
    signals = signals[signals["signal"].fillna(False)].copy()
    signals = signals.rename(columns={"trading_date": "signal_date"})
    signals = signals.merge(
        CATALOG,
        on="factor_id",
        how="left",
        validate="many_to_one",
    )
    signals["signal_date"] = pd.to_datetime(
        signals["signal_date"]
    ).dt.normalize()

    # 移仓监控按交易日唯一，故每条因子信号使用 many-to-one 合并。
    roll_monitor = load_roll_monitor()
    signals = signals.merge(
        roll_monitor,
        left_on="signal_date",
        right_on="trade_date",
        how="left",
        validate="many_to_one",
    )

    # 监控缺失时采用 fail-safe：保留信号行，但强制回测阶段屏蔽。
    missing_monitor_mask = signals["trade_date"].isna()
    missing_monitor_dates = int(
        signals.loc[missing_monitor_mask, "signal_date"].nunique()
    )
    signals["roll_status"] = signals["roll_status"].fillna("NO_MONITOR_DATA")
    signals["block_signal"] = (
        signals["block_signal"]
        .astype("boolean")
        .fillna(True)
        .astype(bool)
    )
    blocked_signal_rows = int(signals["block_signal"].sum())
    blocked_signal_dates = int(
        signals.loc[signals["block_signal"], "signal_date"].nunique()
    )

    signals = signals[
        [
            "signal_date",
            "trade_date",
            "roll_status",
            "block_signal",
            "factor_id",
            "display_name",
            "category",
            "hold_days",
            "direction",
            "signal",
        ]
    ].sort_values(["signal_date", "factor_id"]).reset_index(drop=True)
    return (
        signals,
        roll_monitor,
        missing_monitor_dates,
        blocked_signal_rows,
        blocked_signal_dates,
    )


def _build_quality_frame(
    daily: pd.DataFrame,
    minute: pd.DataFrame,
    events: pd.DataFrame,
    threshold_audit: pd.DataFrame,
    roll_monitor: pd.DataFrame,
    signals: pd.DataFrame,
    missing_monitor_dates: int,
    blocked_signal_rows: int,
    blocked_signal_dates: int,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "check": "validated_inputs",
                "status": "PASS",
                "detail": (
                    f"daily={len(daily)}, minute={len(minute)}, "
                    f"events={len(events)}"
                ),
            },
            {
                "check": "catalog_count",
                "status": "PASS",
                "detail": f"exactly {len(CATALOG)} semantic factors",
            },
            {
                "check": "historical_thresholds",
                "status": "PASS",
                "detail": (
                    f"{int(threshold_audit['rows_checked'].sum())} "
                    "threshold rows checked with source date < signal date"
                ),
            },
            {
                "check": "roll_monitor_merge",
                "status": "PASS" if missing_monitor_dates == 0 else "WARN",
                "detail": (
                    f"monitor_rows={len(roll_monitor)}, "
                    f"missing_signal_dates={missing_monitor_dates}, "
                    f"blocked_signal_rows={blocked_signal_rows}, "
                    f"blocked_signal_dates={blocked_signal_dates}"
                ),
            },
            {
                "check": "signal_rows",
                "status": "PASS",
                "detail": f"{len(signals)} triggered signal rows",
            },
        ]
    )


def _write_signal_workbook(
    path: Path,
    signals: pd.DataFrame,
    quality: pd.DataFrame,
    threshold_audit: pd.DataFrame,
) -> None:
    """原子替换信号工作簿；写入失败时保留原文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.stem}.{uuid4().hex}.tmp{path.suffix}"
    )
    try:
        with pd.ExcelWriter(temporary, engine="openpyxl") as writer:
            signals.to_excel(writer, sheet_name="signals", index=False)
            CATALOG.to_excel(writer, sheet_name="factor_catalog", index=False)
            quality.to_excel(writer, sheet_name="build_quality", index=False)
            threshold_audit.to_excel(
                writer,
                sheet_name="threshold_audit",
                index=False,
            )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def build_signal_workbook(
    output_path: str | Path = SIGNAL_XLSX,
    log_root: str | Path = FACTOR_LOG_DIR,
) -> SignalBuildResult:
    """运行完整信号构建流程并返回输出统计。"""
    output = Path(output_path)
    run_id = f"build_signals_{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex[:8]}"
    run_dir = Path(log_root) / run_id
    logger = _create_logger(run_dir)
    summary_path = run_dir / "run_summary.json"
    started_at = datetime.now()

    try:
        logger.info("开始读取 validated 输入")
        daily, minute, events = load_inputs()
        logger.info(
            "输入读取完成: daily=%d, minute=%d, events=%d",
            len(daily),
            len(minute),
            len(events),
        )

        logger.info("开始计算分钟分段、反转、持仓和事件特征")
        minute_ready = prepare_minute(minute)
        features = build_segment_features(minute_ready)
        features = add_reversal_and_v2(features)
        features = add_oi_trend_event_filters(features, daily, events)
        features = add_semantic_triggers(features)
        threshold_audit = assert_historical_thresholds(features, daily)
        logger.info(
            "特征计算完成: feature_rows=%d, threshold_rows=%d",
            len(features),
            int(threshold_audit["rows_checked"].sum()),
        )

        (
            signals,
            roll_monitor,
            missing_monitor_dates,
            blocked_signal_rows,
            blocked_signal_dates,
        ) = _build_signal_frame(features)
        quality = _build_quality_frame(
            daily=daily,
            minute=minute,
            events=events,
            threshold_audit=threshold_audit,
            roll_monitor=roll_monitor,
            signals=signals,
            missing_monitor_dates=missing_monitor_dates,
            blocked_signal_rows=blocked_signal_rows,
            blocked_signal_dates=blocked_signal_dates,
        )

        logger.info("开始写出 Excel: %s", output.resolve())
        _write_signal_workbook(output, signals, quality, threshold_audit)
        result = SignalBuildResult(
            output_path=output,
            log_dir=run_dir,
            signal_rows=len(signals),
            signal_dates=signals["signal_date"].nunique(),
            factor_count=len(CATALOG),
            blocked_signal_rows=blocked_signal_rows,
            blocked_signal_dates=blocked_signal_dates,
            missing_monitor_dates=missing_monitor_dates,
        )
        summary = {
            "status": "success",
            "started_at": started_at,
            "finished_at": datetime.now(),
            **result.to_dict(),
        }
        _atomic_write_json(summary, summary_path)
        logger.info(
            "信号构建完成: rows=%d, dates=%d, factors=%d",
            result.signal_rows,
            result.signal_dates,
            result.factor_count,
        )
        logger.info(
            "移仓监控: blocked_rows=%d, blocked_dates=%d, missing_dates=%d",
            result.blocked_signal_rows,
            result.blocked_signal_dates,
            result.missing_monitor_dates,
        )
        return result
    except Exception as exc:
        logger.exception("信号构建失败: %s", exc)
        _atomic_write_json(
            {
                "status": "error",
                "started_at": started_at,
                "finished_at": datetime.now(),
                "output_path": output,
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
        description="构建 CICC 尾盘反转九类信号并输出 Excel"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=SIGNAL_XLSX,
        help=f"输出工作簿，默认：{SIGNAL_XLSX}",
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


def main(argv: list[str] | None = None) -> SignalBuildResult:
    configure_console_encoding()
    args = parse_args(argv)
    result = build_signal_workbook(
        output_path=args.output,
        log_root=args.log_root,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    main()
