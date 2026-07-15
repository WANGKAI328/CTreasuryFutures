"""Backtest engine for the final nine close-session reversal factors."""

from dataclasses import dataclass
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ...paths import FACTOR_OUTPUT_DIR, FACTOR_ROOT, FACTOR_WORKING_DIR


SIGNALS_WORKBOOK: Final[str] = "final_9_category_signals.xlsx"
BACKTEST_WORKBOOK: Final[str] = "final_9_category_backtest.xlsx"
POST_OPEN_SWITCH: Final[pd.Timestamp] = pd.Timestamp("2020-07-20")
ROUND_TRIP_COST_BP: Final[float] = 1.0
WORKBOOK_SHEETS: Final[tuple[str, ...]] = ("trades", "factor_stats", "category_stats", "overall", "yearly", "dropped_signals")
TRADE_COLUMNS: Final[list[str]] = ["factor_id", "signal_date", "trade_date", "roll_status", "block_signal", "entry_date", "exit_date", "hold_days", "direction", "entry_price", "exit_price", "pnl_price", "pnl_bp", "pnl_bp_net"]
EXPECTED_FACTOR_IDS: Final[frozenset[str]] = frozenset(
    {
        "range_reversal_tval_hedge_h7",
        "range_reversal_donch_hedge_event_h7",
        "closing_capital_oi_accel_h3",
        "closing_capital_oi_accel_h5",
        "closing_capital_oi_accel_h7",
        "trend_contrarian_drop_rv_event_h3",
        "trend_contrarian_drop_atr_h3",
        "trend_contrarian_short_cover_h3",
        "d_segment_consistency_h5",
    }
)
FACTOR_PLOT_STYLE: Final[dict[str, tuple[str, str, str]]] = {
    "range_reversal_tval_hedge_h7": ("S2", "#1f77b4", "star"),
    "range_reversal_donch_hedge_event_h7": ("S3", "#1f77b4", "star"),
    "closing_capital_oi_accel_h3": ("G2-OIa-h3", "#2ca02c", "star"),
    "closing_capital_oi_accel_h5": ("G2-OIa-h5", "#2ca02c", "star"),
    "closing_capital_oi_accel_h7": ("G2-OIa-h7", "#2ca02c", "star"),
    "trend_contrarian_drop_rv_event_h3": ("S1", "#ff7f0e", "triangle-down"),
    "trend_contrarian_drop_atr_h3": ("S5", "#ff7f0e", "triangle-down"),
    "trend_contrarian_short_cover_h3": ("G2-scdom", "#ff7f0e", "triangle-down"),
    "d_segment_consistency_h5": ("S6", "#9467bd", "diamond"),
}


@dataclass(frozen=True, slots=True)
class BacktestInputError(ValueError):
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class SignalFilterConfig:
    """回测前的信号筛选配置；None 表示该维度不限制。"""

    exclude_blocked: bool = True
    factor_ids: frozenset[str] | None = None
    categories: frozenset[str] | None = None
    roll_statuses: frozenset[str] | None = None
    signal_start_date: str | pd.Timestamp | None = None
    signal_end_date: str | pd.Timestamp | None = None


@dataclass(frozen=True, slots=True)
class BacktestResult:
    trades: pd.DataFrame
    factor_stats: pd.DataFrame
    category_stats: pd.DataFrame
    overall: pd.DataFrame
    yearly: pd.DataFrame
    dropped_signals: pd.DataFrame
    trading_calendar: pd.DatetimeIndex
    combined_figure: go.Figure


def run_backtest(package_dir: Path | None = None, signal_filter: SignalFilterConfig | None = None) -> BacktestResult:
    root = Path(package_dir).resolve() if package_dir is not None else FACTOR_ROOT
    output_dir = root / "output" if package_dir is not None else FACTOR_OUTPUT_DIR
    working_dir = root / "working" if package_dir is not None else FACTOR_WORKING_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    signal_path = output_dir / SIGNALS_WORKBOOK
    minute_path = working_dir / "validated_minute.parquet"
    daily_path = working_dir / "validated_daily.parquet"
    if not signal_path.is_file():
        raise BacktestInputError(f"missing factor workbook: {signal_path}")
    if not minute_path.is_file():
        raise BacktestInputError(f"missing validated minute parquet: {minute_path}")
    if not daily_path.is_file():
        raise BacktestInputError(f"missing validated daily parquet: {daily_path}")
    all_signals = _load_signals(signal_path)
    signals, filter_dropped = _apply_signal_filters(all_signals, signal_filter or SignalFilterConfig())
    catalog = pd.read_excel(signal_path, sheet_name="factor_catalog")
    minute = _load_minute(minute_path)
    daily = _load_daily(daily_path)
    price_frame = _daily_prices(daily)
    segment_prices = _segment_prices(minute)
    trades, execution_dropped = _build_trades(signals, segment_prices)
    dropped_signals = pd.concat([filter_dropped, execution_dropped], ignore_index=True, sort=False)
    trades = trades.merge(catalog[["factor_id", "display_name", "category"]].drop_duplicates("factor_id"), on="factor_id", how="left")
    trades = trades.sort_values(["exit_date", "signal_date", "factor_id"]).reset_index(drop=True)
    dropped_signals = _format_dropped_signals(dropped_signals)
    factor_stats = _factor_stats(trades)
    category_stats = _category_stats(trades)
    overall = _overall_stats(all_signals, signals, trades, dropped_signals)
    yearly = _yearly_stats(trades)
    _write_workbook(output_dir / BACKTEST_WORKBOOK, trades, factor_stats, category_stats, overall, yearly, dropped_signals)
    return BacktestResult(
        trades=trades,
        factor_stats=factor_stats,
        category_stats=category_stats,
        overall=overall,
        yearly=yearly,
        dropped_signals=dropped_signals,
        trading_calendar=pd.DatetimeIndex(segment_prices["trading_date"].drop_duplicates().sort_values()),
        combined_figure=_combined_figure(price_frame, trades),
    )


def _parse_block_signal(value: object) -> bool:
    """兼容 Excel 中的布尔值、0/1 和 TRUE/FALSE 文本；缺失值按需要屏蔽处理。"""
    if pd.isna(value):
        return True
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, float, np.integer, np.floating)) and value in (0, 1):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "t", "yes", "y", "1"}:
        return True
    if text in {"false", "f", "no", "n", "0"}:
        return False
    raise BacktestInputError(f"invalid block_signal value: {value!r}")


def _load_signals(path: Path) -> pd.DataFrame:
    signals = pd.read_excel(path, sheet_name="signals")
    required = {"signal_date", "trade_date", "roll_status", "block_signal", "factor_id", "display_name", "category", "hold_days", "direction", "signal"}
    missing = required.difference(signals.columns)
    if missing:
        raise BacktestInputError(f"signals sheet missing required columns: {sorted(missing)}")
    out = signals.copy()
    out["signal_date"] = pd.to_datetime(out["signal_date"]).dt.normalize()
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.normalize()
    out["roll_status"] = out["roll_status"].fillna("NO_MONITOR_DATA").astype(str)
    out["block_signal"] = out["block_signal"].map(_parse_block_signal).astype(bool)
    unknown = sorted(set(out["factor_id"]).difference(EXPECTED_FACTOR_IDS))
    if unknown:
        raise BacktestInputError(f"unknown factor: {unknown}")
    short_signal = out["signal"].isin([-1, True])
    out = out[short_signal & (out["direction"] == "short")].copy()
    if out.empty:
        raise BacktestInputError("signals sheet has no short signals")
    out["hold_days"] = out["hold_days"].astype(int)
    out["signal"] = -1
    return out


def _apply_signal_filters(signals: pd.DataFrame, config: SignalFilterConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """在构造 trades 前筛选信号，并保留每条被排除信号的具体原因。"""
    reasons = pd.Series("", index=signals.index, dtype=object)

    def reject(condition: pd.Series, label: str) -> None:
        condition = condition.fillna(True).astype(bool)
        current = reasons.loc[condition]
        reasons.loc[condition] = np.where(current.eq(""), label, current + ";" + label)

    if config.exclude_blocked:
        reject(signals["block_signal"], "blocked_by_roll_monitor")
    if config.factor_ids is not None:
        reject(~signals["factor_id"].isin(config.factor_ids), "factor_not_selected")
    if config.categories is not None:
        reject(~signals["category"].isin(config.categories), "category_not_selected")
    if config.roll_statuses is not None:
        reject(~signals["roll_status"].isin(config.roll_statuses), "roll_status_not_selected")
    if config.signal_start_date is not None:
        start_date = pd.Timestamp(config.signal_start_date).normalize()
        reject(signals["signal_date"] < start_date, "before_signal_start_date")
    if config.signal_end_date is not None:
        end_date = pd.Timestamp(config.signal_end_date).normalize()
        reject(signals["signal_date"] > end_date, "after_signal_end_date")

    keep = reasons.eq("")
    selected = signals.loc[keep].copy().reset_index(drop=True)
    filtered_out = signals.loc[~keep].copy()
    filtered_out["reason"] = reasons.loc[~keep].values
    filtered_out = filtered_out.reset_index(drop=True)
    print(f"信号筛选：输入={len(signals)}，保留={len(selected)}，筛除={len(filtered_out)}")
    if selected.empty:
        raise BacktestInputError("signal filters removed every signal; please relax SignalFilterConfig")
    return selected, filtered_out


def _infer_vwap_amount_multiplier(minute: pd.DataFrame) -> tuple[float, float]:
    volume = minute["vol"].replace(0, np.nan)
    fallback = minute[["open_adj", "high_adj", "low_adj", "close_adj"]].mean(axis=1)
    raw_adjusted = minute["amount"] / volume * minute["adj_factor"]
    ratio = (raw_adjusted / fallback).replace([np.inf, -np.inf], np.nan)
    ratio = ratio[ratio.gt(0)].dropna()
    if ratio.empty:
        raise BacktestInputError("cannot infer VWAP amount unit from amount/volume and adjusted OHLC")
    median_ratio = float(ratio.median())
    candidates = (1.0, 10_000.0)
    multiplier = min(candidates, key=lambda value: abs(np.log10(median_ratio / value)))
    if abs(np.log10(median_ratio / multiplier)) > np.log10(2.0):
        raise BacktestInputError(f"unknown VWAP amount unit: median(amount/volume/OHLC)={median_ratio:.6g}")
    return multiplier, median_ratio


def _load_minute(path: Path) -> pd.DataFrame:
    minute = pd.read_parquet(path).copy()
    if "vol" not in minute.columns and "volume" in minute.columns:
        minute["vol"] = minute["volume"]
    required = {"datetime", "trading_date", "amount", "vol", "adj_factor", "open_adj", "high_adj", "low_adj", "close_adj"}
    missing = required.difference(minute.columns)
    if missing:
        raise BacktestInputError(f"validated minute parquet missing required columns: {sorted(missing)}")
    minute["datetime"] = pd.to_datetime(minute["datetime"])
    minute["trading_date"] = pd.to_datetime(minute["trading_date"]).dt.normalize()
    minute["time"] = minute["datetime"].dt.strftime("%H:%M")
    fallback = minute[["open_adj", "high_adj", "low_adj", "close_adj"]].mean(axis=1)
    multiplier, median_ratio = _infer_vwap_amount_multiplier(minute)
    print(f"VWAP amount unit detected: multiplier={multiplier:,.0f}, median_ratio={median_ratio:,.3f}")
    adjusted_vwap = minute["amount"] / minute["vol"].replace(0, np.nan) * minute["adj_factor"] / multiplier
    valid = adjusted_vwap.notna() & np.isfinite(adjusted_vwap) & adjusted_vwap.gt(0)
    minute["minute_vwap_adj"] = adjusted_vwap.where(valid, fallback)
    return minute


def _load_daily(path: Path) -> pd.DataFrame:
    daily = pd.read_parquet(path).copy()
    date_col = "trade_date" if "trade_date" in daily.columns else "trading_date"
    if date_col not in daily.columns or "close_adj" not in daily.columns:
        raise BacktestInputError("validated daily parquet missing trade_date/trading_date or close_adj")
    daily["trade_date"] = pd.to_datetime(daily[date_col]).dt.normalize()
    return daily


def _daily_prices(daily: pd.DataFrame) -> pd.DataFrame:
    columns = ["trade_date", "close_adj"]
    if "is_roll" in daily.columns:
        columns.append("is_roll")
    elif "contract" in daily.columns:
        columns.append("contract")
    out = daily.sort_values("trade_date")[columns].drop_duplicates("trade_date").reset_index(drop=True)
    if "is_roll" not in out.columns:
        out["is_roll"] = out["contract"].ne(out["contract"].shift()) if "contract" in out.columns else False
        if not out.empty:
            out.loc[out.index[0], "is_roll"] = False
    return out.drop(columns=["contract"], errors="ignore")


def _segment_prices(minute: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, pd.Timestamp | float]] = []
    for trading_date, group in minute.groupby("trading_date", sort=True):
        open_start, open_end = ("09:30", "09:34") if trading_date >= POST_OPEN_SWITCH else ("09:15", "09:19")
        rows.append({"trading_date": trading_date, "open5_vwap": _vwap(group, open_start, open_end), "close_vwap": _vwap(group, "15:15", "15:15")})
    out = pd.DataFrame(rows).sort_values("trading_date").reset_index(drop=True)
    return out


def _vwap(frame: pd.DataFrame, start: str, end: str) -> float:
    window = frame[(frame["time"] >= start) & (frame["time"] <= end)]
    if window.empty:
        return float("nan")
    weights = window["vol"].clip(lower=0)
    if float(weights.sum()) > 0.0:
        return float(np.average(window["minute_vwap_adj"], weights=weights))
    return float(window["minute_vwap_adj"].mean())


def _build_trades(signals: pd.DataFrame, prices: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    calendar = list(pd.to_datetime(prices["trading_date"]))
    position = {date: index for index, date in enumerate(calendar)}
    rows: list[dict[str, str | pd.Timestamp | int | float]] = []
    dropped: list[dict[str, str | pd.Timestamp | int]] = []
    for row in signals.itertuples(index=False):
        signal_date = pd.Timestamp(row.signal_date)
        if signal_date not in position:
            raise BacktestInputError(f"out-of-calendar signal: {signal_date.date()}")
        entry_index = position[signal_date] + 1
        exit_index = position[signal_date] + int(row.hold_days)
        if entry_index >= len(calendar) or exit_index >= len(calendar):
            dropped.append(
                {
                    "signal_date": signal_date,
                    "trade_date": row.trade_date,
                    "roll_status": row.roll_status,
                    "block_signal": bool(row.block_signal),
                    "factor_id": row.factor_id,
                    "display_name": row.display_name,
                    "category": row.category,
                    "hold_days": int(row.hold_days),
                    "direction": row.direction,
                    "signal": int(row.signal) if row.signal in (-1, 1, 0) else str(row.signal),
                    "reason": "insufficient_exit_history",
                    "signal_position": position[signal_date],
                    "required_entry_position": entry_index,
                    "required_exit_position": exit_index,
                    "available_last_position": len(calendar) - 1,
                    "available_last_date": calendar[-1],
                }
            )
            continue
        entry = prices.iloc[entry_index]
        exit_row = prices.iloc[exit_index]
        entry_price = float(entry["open5_vwap"])
        exit_price = float(exit_row["close_vwap"])
        if np.isnan(entry_price) or np.isnan(exit_price):
            raise BacktestInputError(f"validated minute parquet lacks required open5 or close_vwap windows for signal: {signal_date.date()}")
        pnl_price = -1.0 * (exit_price - entry_price)
        pnl_bp = pnl_price / entry_price * 10_000.0
        rows.append({"factor_id": row.factor_id, "signal_date": signal_date, "trade_date": row.trade_date, "roll_status": row.roll_status, "block_signal": bool(row.block_signal), "entry_date": entry["trading_date"], "exit_date": exit_row["trading_date"], "hold_days": int(row.hold_days), "direction": -1, "entry_price": entry_price, "exit_price": exit_price, "pnl_price": pnl_price, "pnl_bp": pnl_bp, "pnl_bp_net": pnl_bp - ROUND_TRIP_COST_BP})
    return pd.DataFrame(rows, columns=TRADE_COLUMNS), pd.DataFrame(dropped)


def _format_dropped_signals(dropped_signals: pd.DataFrame) -> pd.DataFrame:
    columns = ["signal_date", "trade_date", "roll_status", "block_signal", "factor_id", "display_name", "category", "hold_days", "direction", "signal", "reason", "signal_position", "required_entry_position", "required_exit_position", "available_last_position", "available_last_date"]
    if dropped_signals.empty:
        return pd.DataFrame(columns=columns)
    return dropped_signals.reindex(columns=columns).sort_values(["signal_date", "factor_id"]).reset_index(drop=True)


def _factor_stats(trades: pd.DataFrame) -> pd.DataFrame:
    return _group_stats(trades, ["factor_id", "display_name"])


def _category_stats(trades: pd.DataFrame) -> pd.DataFrame:
    return _group_stats(trades, ["category"])


def _group_stats(trades: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=[*keys, "trade_count", "win_rate", "avg_pnl_bp", "total_pnl_bp"])
    grouped = trades.groupby(keys, dropna=False)["pnl_bp_net"]
    out = grouped.agg(trade_count="count", avg_pnl_bp="mean", total_pnl_bp="sum").reset_index()
    wins = grouped.apply(lambda values: float((values > 0).mean())).rename("win_rate").reset_index()
    return out.merge(wins, on=keys)[[*keys, "trade_count", "win_rate", "avg_pnl_bp", "total_pnl_bp"]]


def _overall_stats(all_signals: pd.DataFrame, selected_signals: pd.DataFrame, trades: pd.DataFrame, dropped_signals: pd.DataFrame) -> pd.DataFrame:
    portfolio = _portfolio_path(trades)
    filter_dropped_count = int(dropped_signals["reason"].fillna("").ne("insufficient_exit_history").sum()) if not dropped_signals.empty else 0
    metrics = {
        "input_signal_count": len(all_signals),
        "selected_signal_count": len(selected_signals),
        "filter_dropped_count": filter_dropped_count,
        "executable_trade_count": len(trades),
        "dropped_signal_count": len(dropped_signals),
        "win_rate": float((trades["pnl_bp_net"] > 0).mean()),
        "avg_pnl_bp": float(trades["pnl_bp_net"].mean()),
        "total_pnl_bp": float(trades["pnl_bp_net"].sum()),
        "max_drawdown_bp": float(portfolio["drawdown_bp"].min()),
    }
    return pd.DataFrame({"metric": list(metrics), "value": list(metrics.values())})


def _yearly_stats(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["year", "trade_count", "pnl_bp_net"])
    out = trades.assign(year=trades["exit_date"].dt.year).groupby("year")["pnl_bp_net"].agg(trade_count="count", pnl_bp_net="sum").reset_index()
    return out.sort_values("year")


def _portfolio_path(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["exit_date", "pnl_bp_net", "cum_pnl_bp", "drawdown_bp"])
    out = trades.sort_values("exit_date")[["exit_date", "pnl_bp_net"]].copy()
    out["cum_pnl_bp"] = out["pnl_bp_net"].cumsum()
    out["drawdown_bp"] = out["cum_pnl_bp"] - out["cum_pnl_bp"].cummax()
    return out


def _write_workbook(path: Path, trades: pd.DataFrame, factor_stats: pd.DataFrame, category_stats: pd.DataFrame, overall: pd.DataFrame, yearly: pd.DataFrame, dropped_signals: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path) as writer:
        trades.to_excel(writer, sheet_name="trades", index=False)
        factor_stats.to_excel(writer, sheet_name="factor_stats", index=False)
        category_stats.to_excel(writer, sheet_name="category_stats", index=False)
        overall.to_excel(writer, sheet_name="overall", index=False)
        yearly.to_excel(writer, sheet_name="yearly", index=False)
        dropped_signals.to_excel(writer, sheet_name="dropped_signals", index=False)


def _combined_figure(price_frame: pd.DataFrame, trades: pd.DataFrame) -> go.Figure:
    plot_prices = price_frame[price_frame["trade_date"] >= POST_OPEN_SWITCH].copy()
    plot_trades = trades[trades["signal_date"] >= POST_OPEN_SWITCH].copy()
    daily_pnl = (
        plot_trades.groupby("exit_date", as_index=False)["pnl_bp_net"]
        .sum()
        .sort_values("exit_date")
    )
    daily_pnl["cum_pnl_bp"] = daily_pnl["pnl_bp_net"].cumsum()
    daily_pnl["drawdown_bp"] = daily_pnl["cum_pnl_bp"] - daily_pnl["cum_pnl_bp"].cummax()

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.45, 0.30, 0.25],
        vertical_spacing=0.06,
        subplot_titles=("T close_adj + 信号标注", "累积净 PnL (bp)", "回撤 (bp)"),
    )
    fig.add_trace(
        go.Scatter(
            x=plot_prices["trade_date"],
            y=plot_prices["close_adj"],
            mode="lines",
            name="T close",
            line={"color": "#444", "width": 1.2},
            legendgroup="price",
            hovertemplate="%{x|%Y-%m-%d}<br>close_adj=%{y:.3f}<extra>T close</extra>",
        ),
        row=1,
        col=1,
    )

    daily_close = plot_prices.set_index("trade_date")["close_adj"]
    for offset, (factor_id, (short_name, color, symbol)) in enumerate(FACTOR_PLOT_STYLE.items()):
        factor_trades = plot_trades[plot_trades["factor_id"] == factor_id].sort_values("signal_date")
        if factor_trades.empty:
            continue
        signal_prices = factor_trades["signal_date"].map(daily_close) + offset * 0.1
        display_name = str(factor_trades["display_name"].iloc[0])
        hover = [
            (
                f"{short_name} · {display_name}<br>"
                f"signal={row.signal_date:%Y-%m-%d}<br>"
                f"entry={row.entry_date:%Y-%m-%d}<br>"
                f"exit={row.exit_date:%Y-%m-%d}<br>"
                f"net={row.pnl_bp_net:+.1f} bp"
            )
            for row in factor_trades.itertuples(index=False)
        ]
        fig.add_trace(
            go.Scatter(
                x=factor_trades["signal_date"],
                y=signal_prices,
                mode="markers",
                name=f"{short_name} · {display_name} ({len(factor_trades)})",
                marker={
                    "symbol": symbol,
                    "size": 10,
                    "color": color,
                    "line": {"width": 0.6, "color": "#222"},
                },
                legendgroup=factor_id,
                hovertext=hover,
                hoverinfo="text",
            ),
            row=1,
            col=1,
        )

    for roll_date in plot_prices.loc[plot_prices["is_roll"].fillna(False), "trade_date"]:
        fig.add_vline(
            x=roll_date,
            line={"color": "rgba(127,140,141,0.2)", "dash": "dot", "width": 0.8},
            row=1,
            col=1,
        )

    fig.add_trace(
        go.Scatter(
            x=daily_pnl["exit_date"],
            y=daily_pnl["cum_pnl_bp"],
            mode="lines",
            name="累积 PnL",
            line={"color": "#1a1a2e", "width": 1.5},
            legendgroup="pnl",
            hovertemplate="%{x|%Y-%m-%d}<br>cum=%{y:+.1f} bp<extra>累积 PnL</extra>",
        ),
        row=2,
        col=1,
    )
    fig.add_hline(y=0, line_dash="dot", line_color="gray", row=2, col=1)
    fig.add_trace(
        go.Scatter(
            x=daily_pnl["exit_date"],
            y=daily_pnl["drawdown_bp"],
            mode="lines",
            fill="tozeroy",
            name="回撤",
            line={"color": "#d62728", "width": 1},
            fillcolor="rgba(214,39,40,0.25)",
            legendgroup="drawdown",
            hovertemplate="%{x|%Y-%m-%d}<br>drawdown=%{y:+.1f} bp<extra>回撤</extra>",
        ),
        row=3,
        col=1,
    )
    fig.add_hline(y=0, line_dash="dot", line_color="gray", row=3, col=1)

    unique_signal_days = plot_trades["signal_date"].nunique()
    fig.update_layout(
        title=(
            "四大类信号综合 — 价格/信号 · 累积 PnL · 回撤"
            f"<br><sup>2020-07-20 以来：{len(plot_trades)} 笔交易，{unique_signal_days} 个信号日</sup>"
        ),
        template="plotly_white",
        height=1200,
        width=1600,
        hovermode="x unified",
        legend={
            "orientation": "v",
            "yanchor": "top",
            "y": 1,
            "xanchor": "left",
            "x": 1.02,
            "font": {"size": 9},
        },
        margin={"l": 100, "r": 360, "t": 100, "b": 50},
    )
    fig.update_yaxes(title_text="close_adj", row=1, col=1)
    fig.update_yaxes(title_text="bp", row=2, col=1)
    fig.update_yaxes(title_text="bp", row=3, col=1)
    fig.update_xaxes(showspikes=True, spikemode="across", spikesnap="cursor", spikedash="dot")
    return fig
