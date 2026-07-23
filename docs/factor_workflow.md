# CICC 尾盘反转因子构建与回测指南

本文介绍如何从已更新的 DuckDB 构建因子输入、刷新移仓监控、
使用 Python 生成信号 Excel，并在 Notebook 中完成回测。

完整流程：

```text
更新 DuckDB
→ 数据输入与质量控制 Notebook
→ refresh_roll_monitor.py
→ build_signals.py
→ 信号 Excel
→ 回测 Notebook
```

## 1. 运行前准备

首先确认数据库已经完成增量更新和审计：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode incremental
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode audit
```

安装项目依赖：

```powershell
python -m pip install -e .
```

因子相关目录：

- Notebook：`notebooks/factors/`
- Python 实现：`src/factors/cicc_close_session_reverse/`
- 中间数据：`factors/cicc_close_session_reverse/working/`
- 最终输出：`factors/cicc_close_session_reverse/output/`
- 构建日志：`logs/factors/`
- 移仓 SQL：`factors/cicc_close_session_reverse/sql/`

## 2. 构建质量控制后的因子输入

打开：

```text
notebooks/factors/Data_Input_and_Quality.ipynb
```

首个配置区域的核心参数是：

```python
DATA_SOURCE = "duckdb"
```

可选模式：

- `"duckdb"`：推荐模式，直接读取当前 DuckDB；
- `"excel"`：读取显式指定的外部文件，用于手工研究或对照。

DuckDB 模式使用：

```text
data/database/treasury_futures.duckdb
```

Excel/文件模式默认读取：

- `data/input/T_daily.parquet`
- `data/input/T_minute.csv`
- 可选主力映射和外部复权文件

两种模式共用：

```text
data/input/eco_calendar_filtered.xlsx
```

运行 Notebook 全部单元格后，重点检查：

- 日线和分钟线日期覆盖范围；
- 主力映射是否唯一；
- 日线和分钟线主键是否重复；
- OHLC、成交量、持仓和成交额缺失情况；
- 复权前后价格是否连续；
- 经济事件是否成功标准化。

信号生成依赖以下三个稳定输入：

```text
factors/cicc_close_session_reverse/working/validated_daily.parquet
factors/cicc_close_session_reverse/working/validated_minute.parquet
factors/cicc_close_session_reverse/working/validated_events.parquet
```

如果这三个文件不存在或没有更新到最新日期，不要继续生成信号。

## 3. 刷新移仓监控

从项目根目录运行：

```powershell
python src/factors/cicc_close_session_reverse/refresh_roll_monitor.py
```

安装过本项目后也可以运行：

```powershell
python -m factors.cicc_close_session_reverse.refresh_roll_monitor
```

该程序使用：

```text
factors/cicc_close_session_reverse/sql/roll_migration_monitor.sql
```

在 DuckDB 中创建或刷新：

```text
v_roll_migration_monitor
```

主要状态：

| 状态 | 含义 |
| --- | --- |
| `NORMAL` | 连一成交量和持仓占比均低于观察阈值 |
| `WATCH` | 连一成交量或持仓占比开始上升 |
| `ACTIVE` | 移仓明显，建议屏蔽信号 |
| `CROSSOVER` | 连一占比达到主导水平，建议屏蔽信号 |
| `SWITCH_DAY` | Wind 主力映射当天发生切换 |
| `DATA_INCOMPLETE` | 当天数据不完整，按 fail-safe 屏蔽信号 |

`block_signal=True` 表示该信号日产生的信号默认不进入回测。

程序会自动检查：

- 最新交易日是否存在；
- `data_complete` 是否合理；
- `block_signal` 是否符合移仓状态；
- 是否存在重复 `trade_date`。

每次运行的日志和结构化摘要保存在：

```text
logs/factors/refresh_roll_monitor_<timestamp>_<id>/
```

其中包括：

- `refresh_roll_monitor.log`
- `run_summary.json`

若需要人工查看状态分布和临近移仓窗口，可打开：

```text
notebooks/factors/Roll_Migration_Monitor.ipynb
```

该 Notebook 只读查询已经刷新的视图，不负责构建视图。Notebook 会保留只读连接
`con`；未来需要再次更新数据库前，应执行：

```python
con.close()
```

或直接关闭/重启该 Notebook kernel。

## 4. 使用 Python 构建信号

从项目根目录运行：

```powershell
python src/factors/cicc_close_session_reverse/build_signals.py
```

安装过本项目后也可以运行：

```powershell
python -m factors.cicc_close_session_reverse.build_signals
```

程序执行：

1. 读取三个 `validated_*` Parquet；
2. 计算分钟 A/B/C/D、D1/D2/D3、开盘和尾盘分段特征；
3. 计算反转等级、持仓变化、趋势、波动率和事件距离；
4. 检查滚动阈值只使用信号日前的历史数据；
5. 生成九个因子触发器；
6. 合并 `v_roll_migration_monitor`；
7. 对缺失监控日期采用 fail-safe 屏蔽；
8. 原子写出信号 Excel；
9. 写入构建日志和运行摘要。

默认输出：

```text
factors/cicc_close_session_reverse/output/final_9_category_signals.xlsx
```

工作簿包括：

| Sheet | 内容 |
| --- | --- |
| `signals` | 信号日期、因子、持有期、方向和移仓屏蔽状态 |
| `factor_catalog` | 因子 ID、中文名、分类、持有期和公式说明 |
| `build_quality` | 输入、因子数量、历史阈值和移仓合并检查 |
| `threshold_audit` | 历史阈值来源日期审计 |

构建日志：

```text
logs/factors/build_signals_<timestamp>_<id>/
```

其中包括：

- `build_signals.log`
- `run_summary.json`

成功后应确认：

- `factor_count` 等于 9；
- `signal_rows` 和 `signal_dates` 不应无故变成 0；
- `missing_monitor_dates` 应为 0；
- `build_quality` 中不存在异常状态；
- `blocked_signal_rows` 的变化能够由移仓状态解释。

自定义输出位置：

```powershell
python src/factors/cicc_close_session_reverse/build_signals.py `
  --output "factors/cicc_close_session_reverse/output/signals_test.xlsx"
```

默认回测只读取标准文件名。使用自定义路径时，不会自动替换回测输入。

## 5. 在 Notebook 中运行回测

打开：

```text
notebooks/factors/Backtest_Final_9_Category_Factors.ipynb
```

回测前确认标准信号文件已经生成。

主要筛选配置：

```python
signal_filter = SignalFilterConfig(
    exclude_blocked=True,
    factor_ids=None,
    categories=None,
    roll_statuses={"ACTIVE", "WATCH", "NORMAL"},
    signal_start_date=None,
    signal_end_date=None,
)
```

字段说明：

| 参数 | 作用 |
| --- | --- |
| `exclude_blocked` | 是否排除 `block_signal=True` 的信号，建议保持 `True` |
| `factor_ids` | 只回测指定因子；`None` 表示不限制 |
| `categories` | 只回测指定类别；`None` 表示不限制 |
| `roll_statuses` | 只保留指定移仓状态；`None` 表示不限制 |
| `signal_start_date` | 信号开始日期 |
| `signal_end_date` | 信号结束日期 |

指定单个因子的示例：

```python
signal_filter = SignalFilterConfig(
    exclude_blocked=True,
    factor_ids=frozenset({"closing_capital_oi_accel_h3"}),
    categories=None,
    roll_statuses=None,
    signal_start_date="2022-01-01",
    signal_end_date=None,
)
```

回测默认输出：

```text
factors/cicc_close_session_reverse/output/final_9_category_backtest.xlsx
```

主要结果包括：

- 逐笔交易；
- 无法执行或被筛选的信号；
- 单因子统计；
- 因子类别统计；
- 年度统计；
- 总体统计；
- 价格和信号图；
- 累计净收益和回撤图；
- 同日多信号合并为一份仓位的对照结果。

## 6. 哪些变化需要重跑

| 变化 | 需要执行 |
| --- | --- |
| DuckDB 新增交易日 | 数据质量 Notebook → 移仓监控脚本 → 信号脚本 → 回测 Notebook |
| 经济日历更新 | 先更新数据库经济日历，再执行完整因子流程 |
| 因子公式修改 | 信号脚本 → 回测 Notebook |
| 只修改回测筛选条件 | 只运行回测 Notebook |
| 只修改回测统计或图表 | 只运行回测 Notebook |
| 移仓规则或 SQL 修改 | 移仓监控脚本 → 信号脚本 → 回测 Notebook |

## 7. 常见问题

### 缺少 `validated_*` 文件

完整运行 `Data_Input_and_Quality.ipynb`，并确认最后一个导出单元格成功。

### DuckDB 中不存在移仓监控视图

运行：

```powershell
python src/factors/cicc_close_session_reverse/refresh_roll_monitor.py
```

确认命令返回成功，并检查最新 `refresh_roll_monitor_*/run_summary.json`。

### DuckDB 无法打开或提示被占用

关闭其他写连接。若只读移仓监控 Notebook 仍然打开，在其中执行：

```python
con.close()
```

### Pandas/PyArrow 无法读取 Parquet

信号脚本会自动尝试使用 DuckDB 读取同一 Parquet。如果 Pandas 和 DuckDB
都失败，应重新运行数据质量 Notebook 生成 `validated_*` 文件。

### 信号 Excel 无法覆盖

关闭正在 Excel 中打开的 `final_9_category_signals.xlsx` 后重试。程序采用
原子替换，写入失败时不会留下半个工作簿。

### `missing_monitor_dates` 大于 0

不要忽略该提示。先刷新移仓监控视图，确认视图覆盖到信号最新日期，
然后重新运行信号脚本。

## 8. 推荐日常操作顺序

```text
1. 增量更新并审计 DuckDB
2. 完整运行 Data_Input_and_Quality.ipynb
3. 运行 refresh_roll_monitor.py
4. 运行 build_signals.py
5. 打开 Backtest_Final_9_Category_Factors.ipynb
```
