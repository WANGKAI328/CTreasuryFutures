# 国债期货数据库构建与更新指南

本文介绍如何首次构建、日常增量更新和修复
`data/database/treasury_futures.duckdb`。

数据库采用 CSV-first 流程：Wind 和手工数据必须先保存为可审计 CSV，
DuckDB 再从 CSV 同步。日常更新不会全量重建数据库。

## 1. 数据流与目录

| 内容 | 输入位置 | 处理后位置 |
| --- | --- | --- |
| 月度经济日历原始 Excel | `data/csv/eco_calendar/` | `data/input/eco_calendar_filtered.xlsx`、`data/csv/reference/eco_calendar_filtered.csv` |
| 历史分钟数据 | `data/csv/T_mindf.csv` | `data/csv/minute/<contract>.csv` |
| Wind 合约目录和主力映射 | Wind | `data/csv/reference/` |
| Wind 日线 | Wind | `data/csv/daily/<contract>.csv` |
| Wind 分钟线 | Wind | `data/csv/minute/<contract>.csv` |
| 标准 CSV | `data/csv/` | `data/database/treasury_futures.duckdb` |

其他重要目录：

- DuckDB 备份：`data/backups/duckdb/`
- 数据管线日志：`logs/datapipeline/<run_id>/`
- 数据审计报告：`reports/data_pipeline/`

## 2. 运行前准备

所有命令都应从项目根目录执行。

安装 Python 依赖：

```powershell
python -m pip install -e .
```

需要连接 Wind 的任务还必须满足：

1. Wind 终端已经登录；
2. 当前 Python 环境可以导入 `WindPy`；
3. 没有其他进程占用 DuckDB 写连接。

手工输入要求：

1. 把新下载的经济日历 Excel 放入 `data/csv/eco_calendar/`；
2. 首次建库前确认 `data/csv/T_mindf.csv` 存在；
3. 不要手工修改 `data/csv/reference/`、`daily/` 和 `minute/`
   中已经标准化的数据，指定区间修复应使用补丁命令。

## 3. 运行入口

推荐使用命令行入口：

```text
notebooks/datapipeline/CSV_First_Data_Pipeline.py
```

查看所有参数：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --help
```

也可以使用交互式 Notebook：

```text
notebooks/datapipeline/CSV_First_Data_Pipeline.ipynb
```

Notebook 一次只应打开一个写入开关。命令行的 `--mode` 天然保证一次只执行
一种任务，更适合日常维护。

## 4. 首次全量建库

### 4.1 先检查经济日历

以下命令只处理经济日历，不连接 Wind，也不修改 DuckDB：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode eco
```

运行后检查：

- 是否识别了预期数量的原始 Excel；
- 输出日期范围是否合理；
- `data/input/eco_calendar_filtered.xlsx` 是否生成；
- `data/csv/reference/eco_calendar_filtered.csv` 是否生成。

### 4.2 执行首次建库

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py `
  --mode full `
  --start-date 2015-06-30
```

不传 `--end-time` 时，终点使用程序启动时的当前时间。

首次流程依次执行：

1. 合并经济日历原始 Excel；
2. 从 Wind 获取合约目录和主力合约映射；
3. 将 `T_mindf.csv` 拆分为每个合约一个分钟 CSV；
4. 从 Wind 下载每个合约的日线和后续分钟线；
5. 将历史分钟补丁覆盖到分合约 CSV；
6. 如果旧 DuckDB 已存在，先创建时间戳备份；
7. 从标准 CSV 写入 DuckDB；
8. 构建主力连续日线、主力连续分钟线和复权因子。

首次成功后运行只读审计：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode audit
```

## 5. 日常增量更新

通常只需要运行：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode incremental
```

默认策略：

- 日线重新下载最近 5 个交易日；
- 分钟线重新下载最近 2 个交易日；
- 重叠区间按主键去重覆盖；
- 已结束且不再产生数据的合约不会重复更新；
- 尚未结束或仍活跃的合约继续更新；
- DuckDB 只删除并重写发生变化的合约日期区间；
- 主力连续表只重建受影响日期；
- 每次写库前自动备份 DuckDB。

如需调整回溯窗口：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py `
  --mode incremental `
  --daily-overlap 5 `
  --minute-overlap 2
```

如需将更新终点固定到某个时间：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py `
  --mode incremental `
  --end-time "2026-07-23 15:30:00"
```

更新完成后建议再次运行：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode audit
```

## 6. 补丁功能

### 6.1 使用 `T_mindf.csv` 修复历史交易日

该模式不连接 Wind：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py `
  --mode historical-patch `
  --historical-patch-date 2024-11-21
```

流程为：

```text
T_mindf.csv
→ 覆盖对应合约分钟 CSV
→ 备份 DuckDB
→ 增量覆盖 minute_bars
→ 重建该日主力连续分钟
```

### 6.2 从 Wind 强制重抓指定合约

修复日线：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py `
  --mode wind-patch `
  --dataset daily `
  --contract T2609.CFE `
  --patch-start 2026-07-01 `
  --patch-end 2026-07-23
```

修复分钟线：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py `
  --mode wind-patch `
  --dataset minute `
  --contract T2609.CFE `
  --patch-start "2026-07-01 09:30:00" `
  --patch-end "2026-07-23 15:15:00"
```

分钟请求过于频繁时，可以增加请求间隔：

```powershell
--pause-seconds 0.5
```

补丁仍然遵循“先修改 CSV、再备份、后增量同步 DuckDB”的顺序。

## 7. 如何检查运行结果

只读审计命令：

```powershell
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode audit
```

重点检查：

- `daily_bars` 和 `minute_bars` 的最大日期；
- `main_contract_mapping` 是否更新到最新交易日；
- `main_daily_continuous` 和 `main_minute_continuous` 是否同步；
- 行数是否出现异常减少；
- `eco_calendar` 是否已经创建并包含事件。

每次全量、增量或补丁运行后，还应检查：

```text
logs/datapipeline/<run_id>/
```

其中包括人工日志、结构化事件、逐合约摘要和 `run_summary.json`。

## 8. 常见问题

### WindPy 未连接

确认 Wind 终端已登录，并在当前 Python 环境测试：

```powershell
python -c "from WindPy import w; w.start(); print(w.isconnected())"
```

### DuckDB 被占用

关闭正在持有写连接的 Notebook 或 Python 进程。如果只读移仓监控 Notebook
仍然打开，应在不再查询时执行：

```python
con.close()
```

### Excel 无法覆盖

如果 `eco_calendar_filtered.xlsx` 正在 Excel 中打开，Windows 可能禁止原子替换。
关闭工作簿后重试 `--mode eco`。

### 某个合约更新失败

先查看 `logs/datapipeline/<run_id>/` 中的逐合约摘要。确认 Wind 返回正常后，
使用 `wind-patch` 仅修复该合约和日期区间，不需要重新全量建库。

## 9. 推荐日常操作

```powershell
# 1. 更新数据库
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode incremental

# 2. 检查数据库
python notebooks/datapipeline/CSV_First_Data_Pipeline.py --mode audit
```

只有以下情况需要运行 `full`：

- 第一次构建数据库；
- DuckDB 不存在且需要从 CSV 和 Wind 重新恢复；
- 明确决定重建整个历史数据层。
