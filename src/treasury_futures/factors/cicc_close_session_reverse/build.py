"""CICC close-session reversal factor construction.

Extracted from the build notebook to keep research orchestration concise.
"""

from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

from ...paths import (
    DB_PATH as MONITOR_DB_PATH,
    FACTOR_OUTPUT_DIR as OUTPUT_DIR,
    FACTOR_ROOT as PACKAGE_DIR,
    FACTOR_WORKING_DIR as WORKING_DIR,
    INPUT_DIR,
)

DAILY_PATH = WORKING_DIR / "validated_daily.parquet"
MINUTE_PATH = WORKING_DIR / "validated_minute.parquet"
EVENT_PATH = WORKING_DIR / "validated_events.parquet"
EVENT_FALLBACK_PATH = INPUT_DIR / "eco_calendar_filtered.xlsx"
SIGNAL_XLSX = OUTPUT_DIR / "final_9_category_signals.xlsx"
POST_START = pd.Timestamp("2020-07-20")
ROLL = 20
ROLL_MP = 10
ROLL_PCT = 63
ROLL_PCT_MP = 30
ROLL_H = 63
ROLL_H_MP = 20
SEGMENTS = {
    "A": ("00:00", "13:59"), "B": ("14:00", "14:24"), "C": ("14:25", "14:49"), "D": ("14:50", "15:15"),
    "D1": ("14:50", "14:58"), "D2": ("14:59", "15:06"), "D3": ("15:07", "15:15"),
    "open5_pre": ("09:15", "09:19"), "open5_post": ("09:30", "09:34"), "last2": ("15:14", "15:15"),
}
FACTOR_CATALOG = [
    ("range_reversal_tval_hedge_h7", "震荡市反转：成交额区间对冲", "1. 震荡市反转", 7, "short", "乙二级尾盘反转 + 成交额二十日区间 + 严格对冲两日"),
    ("range_reversal_donch_hedge_event_h7", "震荡市反转：唐奇安区间远事件", "1. 震荡市反转", 7, "short", "乙二级尾盘反转 + 唐奇安四十日区间 + 严格对冲两日 + 远离事件三日"),
    ("closing_capital_oi_accel_h3", "尾盘资金进场：持仓加速三日", "2. 尾盘资金进场", 3, "short", "甲三级尾盘反转 + 持仓加速为正 + 三日持有"),
    ("closing_capital_oi_accel_h5", "尾盘资金进场：持仓加速五日", "2. 尾盘资金进场", 5, "short", "甲三级尾盘反转 + 持仓加速为正 + 五日持有"),
    ("closing_capital_oi_accel_h7", "尾盘资金进场：持仓加速七日", "2. 尾盘资金进场", 7, "short", "甲三级尾盘反转 + 持仓加速为正 + 七日持有"),
    ("trend_contrarian_drop_rv_event_h3", "趋势逆势博弈：强跌高波动近事件", "3. 趋势逆势博弈", 3, "short", "乙二级尾盘反转 + 三日基础做空 + 强跌排除 + 十日实现波动高位 + 近事件一日"),
    ("trend_contrarian_drop_atr_h3", "趋势逆势博弈：强跌ATR中档", "3. 趋势逆势博弈", 3, "short", "乙二级尾盘反转 + 三日基础做空 + 强跌排除 + 十日ATR中档"),
    ("trend_contrarian_short_cover_h3", "趋势逆势博弈：空平主导", "3. 趋势逆势博弈", 3, "short", "乙二级尾盘反转 + 空头回补主导 + 三日持有"),
    ("d_segment_consistency_h5", "D段一致性：尾盘方向确认", "4. D段一致性", 5, "short", "甲三级尾盘反转 + D段方向一致性 + 五日持有"),
]
CATALOG = pd.DataFrame(FACTOR_CATALOG, columns=["factor_id", "display_name", "category", "hold_days", "direction", "formula"])

def require_columns(df: pd.DataFrame, columns: list[str], name: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{name} 缺少必需列: {', '.join(missing)}")


def read_parquet_with_retry(path, label: str, attempts: int = 4, delay_seconds: float = 0.5) -> pd.DataFrame:
    """容忍 notebook1 正在替换文件时出现的短暂 footer 不完整。"""
    import time

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(delay_seconds)
    raise RuntimeError(f"{label} 无法读取: {path}；最后错误: {last_error}") from last_error


def read_events() -> pd.DataFrame:
    fallback_path = globals().get("EVENT_FALLBACK_PATH")
    if EVENT_PATH.is_file():
        try:
            return read_parquet_with_retry(EVENT_PATH, "validated_events")
        except RuntimeError:
            if fallback_path is None or not fallback_path.is_file():
                raise
            print(f"警告: {EVENT_PATH} 暂时不可读，改从 {fallback_path} 重建事件表。")
    elif fallback_path is None or not fallback_path.is_file():
        raise FileNotFoundError(f"事件输入不存在: {EVENT_PATH}; fallback={fallback_path}")

    return pd.read_excel(fallback_path).rename(
        columns={"date": "event_date", "indicator": "event_name", "tf_category": "event_type"}
    )


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing_files = [str(path) for path in [DAILY_PATH, MINUTE_PATH] if not path.is_file()]
    if missing_files:
        raise FileNotFoundError("缺少 notebook1 产出的 validated parquet: " + "; ".join(missing_files))
    daily = read_parquet_with_retry(DAILY_PATH, "validated_daily")
    minute = read_parquet_with_retry(MINUTE_PATH, "validated_minute")
    events = read_events()
    require_columns(daily, ["trade_date", "ts_code", "open_adj", "high_adj", "low_adj", "close_adj", "amount", "volume", "oi"], "validated_daily")
    require_columns(minute, ["datetime", "trading_date", "ts_code", "open_adj", "high_adj", "low_adj", "close_adj", "amount", "volume", "oi"], "validated_minute")
    require_columns(events, ["event_date", "event_name", "event_type"], "validated_events")
    if events.empty:
        raise ValueError("validated_events 为空，事件距离过滤器无法构建")
    daily = daily.copy(); minute = minute.copy(); events = events.copy()
    daily["trade_date"] = pd.to_datetime(daily["trade_date"])
    minute["datetime"] = pd.to_datetime(minute["datetime"])
    minute["trading_date"] = pd.to_datetime(minute["trading_date"])
    events["event_date"] = pd.to_datetime(events["event_date"])
    if minute.duplicated(["datetime", "ts_code"]).any():
        raise ValueError("validated_minute 存在重复 datetime/ts_code")
    if daily["trade_date"].nunique() < ROLL_H:
        raise ValueError("历史长度不足，无法计算历史滚动阈值")
    return daily.sort_values(["ts_code", "trade_date"]).reset_index(drop=True), minute.sort_values(["ts_code", "trading_date", "datetime"]).reset_index(drop=True), events.sort_values("event_date").reset_index(drop=True)


def load_roll_monitor() -> pd.DataFrame:
    """从 DuckDB 读取逐交易日移仓监控字段，供 signals 按日期进行 many-to-one 合并。"""
    if not MONITOR_DB_PATH.is_file():
        raise FileNotFoundError(f"移仓监控数据库不存在: {MONITOR_DB_PATH}")

    # 这里只读取已经持久化在 DuckDB 中的视图，不修改行情或监控数据。
    try:
        monitor_con = duckdb.connect(str(MONITOR_DB_PATH), read_only=True)
    except duckdb.IOException as exc:
        raise RuntimeError(
            "无法只读打开 treasury_futures.duckdb；如果 Roll_Migration_Monitor.ipynb 正在运行旧版本，请先执行 con.close() 或重新运行其第一个代码单元以释放读写锁。"
        ) from exc
    try:
        monitor = monitor_con.execute("""
            SELECT trade_date, roll_status, block_signal
            FROM v_roll_migration_monitor
            ORDER BY trade_date
        """).df()
    except duckdb.CatalogException as exc:
        raise RuntimeError(
            "DuckDB 中不存在 v_roll_migration_monitor；请先运行 Roll_Migration_Monitor.ipynb。"
        ) from exc
    finally:
        monitor_con.close()

    require_columns(monitor, ["trade_date", "roll_status", "block_signal"], "roll_monitor")
    monitor = monitor.copy()
    monitor["trade_date"] = pd.to_datetime(monitor["trade_date"]).dt.normalize()
    if monitor["trade_date"].duplicated().any():
        duplicated_dates = monitor.loc[monitor["trade_date"].duplicated(False), "trade_date"].dt.strftime("%Y-%m-%d").unique()
        raise ValueError(f"移仓监控表存在重复 trade_date: {duplicated_dates[:10].tolist()}")
    return monitor.sort_values("trade_date").reset_index(drop=True)

def trailing_quantile(series: pd.Series, q: float, window: int = ROLL_H, min_periods: int = ROLL_H_MP) -> pd.Series:
    return series.shift(1).rolling(window, min_periods=min_periods).quantile(q)

def rolling_pct(series: pd.Series, window: int = ROLL_PCT, min_periods: int = ROLL_PCT_MP) -> pd.Series:
    return series.rolling(window, min_periods=min_periods).apply(lambda values: (values[:-1] < values.iloc[-1]).mean() if len(values) > 1 else np.nan, raw=False).shift(1)

def infer_vwap_amount_multiplier(minute: pd.DataFrame) -> tuple[float, float]:
    """识别 amount 是价格×成交量，还是还包含期货合约乘数 10,000。"""
    volume = minute["volume"].replace(0, np.nan)
    factor = minute["adj_factor"] if "adj_factor" in minute.columns else 1.0
    fallback = minute[["open_adj", "high_adj", "low_adj", "close_adj"]].mean(axis=1)
    raw_adjusted = minute["amount"] / volume * factor
    ratio = (raw_adjusted / fallback).replace([np.inf, -np.inf], np.nan)
    ratio = ratio[ratio.gt(0)].dropna()
    if ratio.empty:
        raise ValueError("无法根据 amount/volume 与 OHLC 推断 VWAP 成交额单位")
    median_ratio = float(ratio.median())
    candidates = (1.0, 10_000.0)
    multiplier = min(candidates, key=lambda value: abs(np.log10(median_ratio / value)))
    if abs(np.log10(median_ratio / multiplier)) > np.log10(2.0):
        raise ValueError(f"未知的 VWAP 成交额单位: median(amount/volume/OHLC)={median_ratio:.6g}")
    return multiplier, median_ratio

def minute_vwap(minute: pd.DataFrame) -> pd.Series:
    volume = minute["volume"].replace(0, np.nan)
    fallback = minute[["open_adj", "high_adj", "low_adj", "close_adj"]].mean(axis=1)
    factor = minute["adj_factor"] if "adj_factor" in minute.columns else 1.0
    multiplier, median_ratio = infer_vwap_amount_multiplier(minute)
    print(f"VWAP 成交额单位识别: multiplier={multiplier:,.0f}, median_ratio={median_ratio:,.3f}")
    adjusted = minute["amount"] / volume * factor / multiplier
    valid = adjusted.notna() & np.isfinite(adjusted) & adjusted.gt(0)
    return adjusted.where(valid, fallback)

def prepare_minute(minute: pd.DataFrame) -> pd.DataFrame:
    out = minute.copy()
    out["minute_vwap_adj"] = minute_vwap(out)
    out["time"] = out["datetime"].dt.strftime("%H:%M")
    out = out.sort_values(["ts_code", "trading_date", "datetime"]).reset_index(drop=True)
    out["dprice"] = out.groupby(["ts_code", "trading_date"])["close_adj"].diff()
    out["doi"] = out.groupby(["ts_code", "trading_date"])["oi"].diff()
    return out

def segment_vwap(group: pd.DataFrame, start: str, end: str) -> float:
    sub = group[(group["time"] >= start) & (group["time"] <= end)]
    if sub.empty:
        return np.nan
    weights = sub["volume"].clip(lower=0)
    if weights.sum() > 0:
        return float(np.average(sub["minute_vwap_adj"], weights=weights))
    return float(sub["minute_vwap_adj"].mean())

def segment_oi(group: pd.DataFrame, start: str, end: str, segment: str) -> dict[str, float]:
    sub = group[(group["time"] >= start) & (group["time"] <= end)]
    empty = {f"{segment}_{name}": np.nan for name in ["oi_delta", "seg_vol", "oi_long_open_ratio", "oi_short_open_ratio", "oi_long_close_ratio", "oi_short_close_ratio"]}
    if sub.empty:
        return empty
    valid = sub.dropna(subset=["dprice", "doi"])
    total_volume = valid["volume"].sum()
    row = {f"{segment}_oi_delta": float(sub["oi"].iloc[-1] - sub["oi"].iloc[0]), f"{segment}_seg_vol": float(sub["volume"].sum())}
    if total_volume <= 0:
        row.update({key: np.nan for key in empty if key not in row})
        return row
    row[f"{segment}_oi_long_open_ratio"] = float(valid.loc[(valid["dprice"] > 0) & (valid["doi"] > 0), "volume"].sum() / total_volume)
    row[f"{segment}_oi_short_open_ratio"] = float(valid.loc[(valid["dprice"] < 0) & (valid["doi"] > 0), "volume"].sum() / total_volume)
    row[f"{segment}_oi_long_close_ratio"] = float(valid.loc[(valid["dprice"] < 0) & (valid["doi"] < 0), "volume"].sum() / total_volume)
    row[f"{segment}_oi_short_close_ratio"] = float(valid.loc[(valid["dprice"] > 0) & (valid["doi"] < 0), "volume"].sum() / total_volume)
    return row

def build_segment_features(minute: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (trading_date, ts_code), group in minute.groupby(["trading_date", "ts_code"], sort=True):
        open5 = SEGMENTS["open5_post"] if trading_date >= POST_START else SEGMENTS["open5_pre"]
        row = {"trading_date": trading_date, "ts_code": ts_code, "post_20200720": trading_date >= POST_START}
        for segment in ["A", "B", "C", "D", "D1", "D2", "D3"]:
            row[f"{segment}_vwap"] = segment_vwap(group, *SEGMENTS[segment])
            row.update(segment_oi(group, *SEGMENTS[segment], segment))
        row["open5_vwap"] = segment_vwap(group, *open5)
        row["last2_vwap"] = segment_vwap(group, *SEGMENTS["last2"])
        d_sub = group[(group["time"] >= SEGMENTS["D"][0]) & (group["time"] <= SEGMENTS["D"][1])]
        row["D_high"] = float(d_sub["high_adj"].max()) if not d_sub.empty else np.nan
        row["D_low"] = float(d_sub["low_adj"].min()) if not d_sub.empty else np.nan
        row["day_oi_delta"] = float(group["oi"].iloc[-1] - group["oi"].iloc[0])
        row["day_vol"] = float(group["volume"].sum())
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["ts_code", "trading_date"]).reset_index(drop=True)

def rolling_tval(close_log: pd.Series, window: int) -> pd.Series:
    def calc(values: np.ndarray) -> float:
        if len(values) < window or np.any(~np.isfinite(values)):
            return np.nan
        x = np.arange(len(values), dtype=float)
        x = x - x.mean()
        sxx = (x * x).sum()
        if sxx <= 0:
            return np.nan
        beta = (x * (values - values.mean())).sum() / sxx
        resid = values - (values.mean() + beta * x)
        se = np.sqrt((resid ** 2).sum() / (len(values) - 2) / sxx)
        return beta / se if se > 0 else np.nan
    return close_log.rolling(window).apply(calc, raw=True)

def wilder_rma(series: pd.Series, window: int) -> pd.Series:
    return series.ewm(alpha=1.0 / window, adjust=False, min_periods=window).mean()

def adx_dmi(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
    up = high.diff(); down = -low.diff()
    plus_dm = ((up > down) & (up > 0)).astype(float) * up
    minus_dm = ((down > up) & (down > 0)).astype(float) * down
    true_range = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr = wilder_rma(true_range, window)
    pdi = 100 * wilder_rma(plus_dm, window) / atr.replace(0, np.nan)
    mdi = 100 * wilder_rma(minus_dm, window) / atr.replace(0, np.nan)
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return wilder_rma(dx, window)

def ema_align(close: pd.Series) -> pd.Series:
    e8 = close.ewm(span=8, adjust=False).mean(); e21 = close.ewm(span=21, adjust=False).mean(); e55 = close.ewm(span=55, adjust=False).mean()
    return 2 * (((e8 > e21).astype(float) + (e21 > e55).astype(float) + (e8 > e55).astype(float)) / 3.0) - 1

def regime_5(row: pd.Series) -> str | float:
    if pd.isna(row["ema_align"]) or pd.isna(row["tval_60"]) or pd.isna(row["adx_14"]):
        return np.nan
    if row["tval_60"] > 2 and row["ema_align"] > 0.6 and row["adx_14"] > 25:
        return "strong_up"
    if row["tval_60"] < -2 and row["ema_align"] < -0.6 and row["adx_14"] > 25:
        return "strong_down"
    if abs(row["tval_60"]) < 1 and abs(row["ema_align"]) < 0.3 and row["adx_14"] < 20:
        return "range"
    return "weak_up" if row["tval_60"] > 0 else "weak_down"

def add_reversal_and_v2(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    long_rev = (out["B_vwap"] < out["A_vwap"]) & (out["D_vwap"] > out["C_vwap"])
    short_rev = (out["B_vwap"] > out["A_vwap"]) & (out["D_vwap"] < out["C_vwap"])
    out["reversal_dir"] = np.select([long_rev, short_rev], [1, -1], default=0).astype(int)
    out["long_reversal"] = out["reversal_dir"] == 1
    out["short_reversal"] = out["reversal_dir"] == -1
    out["rev_class"] = np.select([out["long_reversal"] & (out["C_vwap"] <= out["B_vwap"]) | out["short_reversal"] & (out["C_vwap"] >= out["B_vwap"]), out["long_reversal"] & (out["C_vwap"] > out["B_vwap"]) | out["short_reversal"] & (out["C_vwap"] < out["B_vwap"])], ["jia", "yi"], default="none")
    ranks = out[["A_vwap", "B_vwap", "C_vwap", "D_vwap"]]
    out["D_rank"] = np.where(out["reversal_dir"] > 0, ranks.rank(axis=1, method="min", ascending=False)["D_vwap"], np.where(out["reversal_dir"] < 0, ranks.rank(axis=1, method="min", ascending=True)["D_vwap"], np.nan))
    out["rev_grade"] = "none"
    out.loc[(out["rev_class"] == "jia") & (out["D_rank"] == 1), "rev_grade"] = "jia1"
    out.loc[(out["rev_class"] == "jia") & (out["D_rank"] == 2), "rev_grade"] = "jia2"
    out.loc[(out["rev_class"] == "jia") & (out["D_rank"] == 3), "rev_grade"] = "jia3"
    out.loc[(out["rev_class"] == "yi") & (out["D_rank"] == 1), "rev_grade"] = "yi1"
    out.loc[(out["rev_class"] == "yi") & (out["D_rank"] >= 2), "rev_grade"] = "yi2"
    out["short_count_3d"] = out.groupby("ts_code")["short_reversal"].transform(lambda s: s.rolling(3, min_periods=1).sum())
    out["long_count_3d"] = out.groupby("ts_code")["long_reversal"].transform(lambda s: s.rolling(3, min_periods=1).sum())
    out["sig_base_3d2"] = np.where(out["long_count_3d"] >= 2, 1, np.where(out["short_count_3d"] >= 2, -1, 0))
    out["D_range"] = out["D_high"] - out["D_low"]
    out["D_range_ma20"] = out.groupby("ts_code")["D_range"].transform(lambda s: s.rolling(ROLL, min_periods=ROLL_MP).mean().shift(1))
    out["mag_bp"] = np.where(out["reversal_dir"] < 0, (out["C_vwap"] - out["D_vwap"]) / out["C_vwap"] * 1e4, np.where(out["reversal_dir"] > 0, (out["D_vwap"] - out["C_vwap"]) / out["C_vwap"] * 1e4, np.nan))
    out["sudden_score"] = np.where(out["reversal_dir"] < 0, (out["D1_vwap"] - out["D3_vwap"]) / out["D_range_ma20"].replace(0, np.nan), np.where(out["reversal_dir"] > 0, (out["D3_vwap"] - out["D1_vwap"]) / out["D_range_ma20"].replace(0, np.nan), np.nan))
    out["d_consistency"] = np.where(out["reversal_dir"] < 0, (out["D1_vwap"] < out["C_vwap"]) & (out["D2_vwap"] < out["D1_vwap"]) & (out["D3_vwap"] < out["D2_vwap"]), np.where(out["reversal_dir"] > 0, (out["D1_vwap"] > out["C_vwap"]) & (out["D2_vwap"] > out["D1_vwap"]) & (out["D3_vwap"] > out["D2_vwap"]), False)).astype(bool)
    out["sudden_pct"] = out.groupby("ts_code")["sudden_score"].transform(rolling_pct)
    out["mag_pct"] = out.groupby("ts_code")["mag_bp"].transform(rolling_pct)
    return out

def add_oi_trend_event_filters(frame: pd.DataFrame, daily: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["ts_code", "trading_date"]).reset_index(drop=True).copy()
    for segment in ["A", "B", "C", "D", "D1", "D2", "D3"]:
        out[f"{segment}_vol_amp"] = out[f"{segment}_seg_vol"] / out.groupby("ts_code")[f"{segment}_seg_vol"].transform(lambda s: s.rolling(ROLL, min_periods=ROLL_MP).mean().shift(1)).replace(0, np.nan)
        out[f"{segment}_oi_delta_detr"] = out[f"{segment}_oi_delta"] - out.groupby("ts_code")[f"{segment}_oi_delta"].transform(lambda s: s.rolling(ROLL, min_periods=ROLL_MP).mean().shift(1))
    out["day_oi_delta_detr"] = out["day_oi_delta"] - out.groupby("ts_code")["day_oi_delta"].transform(lambda s: s.rolling(ROLL, min_periods=ROLL_MP).mean().shift(1))
    out["day_vol_amp"] = out["day_vol"] / out.groupby("ts_code")["day_vol"].transform(lambda s: s.rolling(ROLL, min_periods=ROLL_MP).mean().shift(1)).replace(0, np.nan)
    out["cum_oi"] = out.groupby("ts_code")["day_oi_delta"].cumsum()
    out["oi_tval_20d"] = out.groupby("ts_code")["cum_oi"].transform(lambda s: s.rolling(20).apply(lambda values: np.nan if np.isnan(values).any() else np.polyfit(np.arange(len(values)), values, 1)[0], raw=True).shift(1))
    out["oi_tval_20d_sign"] = np.where(out["oi_tval_20d"].isna(), "na", np.where(out["oi_tval_20d"] < 0, "neg", np.where(out["oi_tval_20d"] > 0, "pos", "zero")))
    ratios = out[["D_oi_long_open_ratio", "D_oi_short_open_ratio", "D_oi_long_close_ratio", "D_oi_short_close_ratio"]]
    out["close_oi_dominant"] = pd.Series(np.nan, index=out.index, dtype=object)
    valid_ratios = ratios[~ratios.isna().all(axis=1)]
    out.loc[valid_ratios.index, "close_oi_dominant"] = valid_ratios.idxmax(axis=1).map({"D_oi_long_open_ratio": "lo", "D_oi_short_open_ratio": "so", "D_oi_long_close_ratio": "lc", "D_oi_short_close_ratio": "sc"})
    out["abc_oi_mean"] = out[["A_oi_delta", "B_oi_delta", "C_oi_delta"]].mean(axis=1)
    out["close_oi_accel"] = out["D_oi_delta"] - out["abc_oi_mean"]
    out["close_oi_accel_sign"] = np.where(out["close_oi_accel"].isna(), "na", np.where(out["close_oi_accel"] < 0, "neg", np.where(out["close_oi_accel"] > 0, "pos", "zero")))
    out["close_d3_extreme"] = out["D3_oi_delta_detr"].abs()
    out["delta_close_D"] = out["D_vwap"] - out["C_vwap"]
    out["H3_day_oi_drop_lo"] = out["day_oi_delta_detr"] <= out.groupby("ts_code")["day_oi_delta_detr"].transform(lambda s: trailing_quantile(s, 0.2))
    out["sig_H5"] = ((out["delta_close_D"] < 0) & ((out["D_seg_vol"] / out["C_seg_vol"]) >= 1.5)).fillna(False)
    daily_sorted = daily.sort_values("trade_date").copy()
    close = daily_sorted["close_adj"]
    high = daily_sorted["high_adj"]
    low = daily_sorted["low_adj"]
    daily_sorted["donch_pos_40"] = (close - low.rolling(40).min()) / (high.rolling(40).max() - low.rolling(40).min()).replace(0, np.nan)
    daily_sorted["tval_20"] = rolling_tval(np.log(close), 20)
    daily_sorted["tval_60"] = rolling_tval(np.log(close), 60)
    daily_sorted["adx_14"] = adx_dmi(high, low, close, 14)
    daily_sorted["ema_align"] = ema_align(close)
    daily_sorted["regime_5"] = daily_sorted.apply(regime_5, axis=1)
    log_ret = np.log(close / close.shift(1))
    daily_sorted["rv_10d"] = log_ret.rolling(10).std() * np.sqrt(252)
    tr = pd.concat([(high - low), (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    daily_sorted["atr_10"] = tr.rolling(10).mean() / close
    for col in ["rv_10d", "atr_10"]:
        low_q = daily_sorted[col].shift(1).rolling(63, min_periods=31).quantile(0.33)
        high_q = daily_sorted[col].shift(1).rolling(63, min_periods=31).quantile(0.67)
        bucket = pd.Series(np.nan, index=daily_sorted.index, dtype=object)
        valid_bucket = daily_sorted[col].notna() & low_q.notna() & high_q.notna()
        bucket[valid_bucket & (daily_sorted[col] < low_q)] = "low"
        bucket[valid_bucket & (daily_sorted[col] >= low_q) & (daily_sorted[col] < high_q)] = "mid"
        bucket[valid_bucket & (daily_sorted[col] >= high_q)] = "high"
        daily_sorted[f"{col}_bucket"] = bucket
    daily_sorted["f_tval20_range"] = daily_sorted["tval_20"].abs() < 1
    daily_sorted["f_donch40_in_range"] = daily_sorted["donch_pos_40"].between(0.3, 0.7)
    daily_sorted["f_drop_strong_down"] = daily_sorted["regime_5"] != "strong_down"
    daily_sorted["f_rv_10d_high"] = daily_sorted["rv_10d_bucket"].eq("high")
    daily_sorted["f_atr_10_mid"] = daily_sorted["atr_10_bucket"].eq("mid")
    trading_dates = pd.Series(pd.to_datetime(daily_sorted["trade_date"]).drop_duplicates().sort_values().to_numpy())
    event_dates = pd.to_datetime(events["event_date"]).drop_duplicates().sort_values()
    event_idx = np.searchsorted(trading_dates.to_numpy(), event_dates.to_numpy(), side="left").clip(0, len(trading_dates) - 1)
    all_idx = np.arange(len(trading_dates)); pos = np.searchsorted(np.array(sorted(set(event_idx.tolist()))), all_idx)
    ev_arr = np.array(sorted(set(event_idx.tolist())))
    dist = np.minimum(np.abs(all_idx - ev_arr[np.clip(pos - 1, 0, len(ev_arr) - 1)]), np.abs(all_idx - ev_arr[np.clip(pos, 0, len(ev_arr) - 1)]))
    distance = pd.Series(dist, index=trading_dates)
    daily_sorted["days_to_event"] = daily_sorted["trade_date"].map(distance).astype("Int64")
    daily_sorted["f_near_event_1d"] = daily_sorted["days_to_event"] <= 1
    daily_sorted["f_far_event_3d"] = daily_sorted["days_to_event"] >= 3
    keep = ["trade_date", "f_tval20_range", "f_donch40_in_range", "f_drop_strong_down", "f_rv_10d_high", "f_atr_10_mid", "f_near_event_1d", "f_far_event_3d", "days_to_event"]
    out = out.merge(daily_sorted[keep].rename(columns={"trade_date": "trading_date"}), on="trading_date", how="left")
    long_rev = out["long_reversal"].astype(int)
    out["long_rev_2d"] = long_rev.shift(1).rolling(2, min_periods=1).sum()
    out["f_hedge_strict_2d"] = out["long_rev_2d"] == 0
    return out

def add_semantic_triggers(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    yi2 = out["rev_grade"].eq("yi2"); jia3 = out["rev_grade"].eq("jia3")
    base_short = out["sig_base_3d2"].fillna(0).astype(int).eq(-1); short_rev = out["short_reversal"].fillna(False).astype(bool)
    out["range_reversal_tval_hedge_h7"] = yi2 & base_short & out["f_tval20_range"].fillna(False) & out["f_hedge_strict_2d"].fillna(False)
    out["range_reversal_donch_hedge_event_h7"] = yi2 & base_short & out["f_donch40_in_range"].fillna(False) & out["f_hedge_strict_2d"].fillna(False) & out["f_far_event_3d"].fillna(False)
    out["closing_capital_oi_accel_h3"] = jia3 & short_rev & out["close_oi_accel_sign"].eq("pos")
    out["closing_capital_oi_accel_h5"] = out["closing_capital_oi_accel_h3"]
    out["closing_capital_oi_accel_h7"] = out["closing_capital_oi_accel_h3"]
    out["trend_contrarian_drop_rv_event_h3"] = yi2 & base_short & out["f_drop_strong_down"].fillna(False) & out["f_rv_10d_high"].fillna(False) & out["f_near_event_1d"].fillna(False)
    out["trend_contrarian_drop_atr_h3"] = yi2 & base_short & out["f_drop_strong_down"].fillna(False) & out["f_atr_10_mid"].fillna(False)
    out["trend_contrarian_short_cover_h3"] = yi2 & short_rev & out["close_oi_dominant"].eq("sc")
    out["d_segment_consistency_h5"] = jia3 & base_short & out["d_consistency"].fillna(False)
    return out

def assert_historical_thresholds(features: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    feature_dates = pd.to_datetime(features["trading_date"])
    previous_feature_date = features.sort_values(["ts_code", "trading_date"]).groupby("ts_code")["trading_date"].shift(1)
    daily_dates = pd.Series(pd.to_datetime(daily["trade_date"]).drop_duplicates().sort_values().to_numpy())
    previous_daily_date = pd.Series(daily_dates.shift(1).to_numpy(), index=daily_dates.to_numpy())
    audit_rows = []

    def check_family(name: str, columns: list[str], source_dates: pd.Series) -> None:
        present = [column for column in columns if column in features.columns]
        if not present:
            return
        active = features[present].notna().any(axis=1)
        comparable = active & source_dates.notna()
        if (pd.to_datetime(source_dates[comparable]) >= feature_dates[comparable]).any():
            raise ValueError(f"historical-only rolling threshold violation: {name}")
        audit_rows.append({
            "threshold_family": name,
            "rows_checked": int(comparable.sum()),
            "latest_signal_date": feature_dates[comparable].max(),
            "latest_threshold_source_date": pd.to_datetime(source_dates[comparable]).max(),
        })

    segment_thresholds = [
        "D_range_ma20", "sudden_pct", "mag_pct", "day_oi_delta_detr", "day_vol_amp", "oi_tval_20d",
        *[f"{segment}_vol_amp" for segment in ["A", "B", "C", "D", "D1", "D2", "D3"]],
        *[f"{segment}_oi_delta_detr" for segment in ["A", "B", "C", "D", "D1", "D2", "D3"]],
        "H3_day_oi_drop_lo",
    ]
    check_family("minute_segment_and_oi_rolls", segment_thresholds, pd.to_datetime(previous_feature_date))
    daily_source_dates = feature_dates.map(previous_daily_date)
    check_family("daily_trend_vol_rolls", ["f_tval20_range", "f_donch40_in_range", "f_rv_10d_high", "f_atr_10_mid"], pd.to_datetime(daily_source_dates))
    audit = pd.DataFrame(audit_rows)
    if audit.empty:
        raise ValueError("historical-only rolling threshold audit produced no rows")
    return audit
