# Treasury Futures

项目按“行情数据管线”和“CICC 尾盘反转因子”两条职责拆分。数据管线路径维护在 `src/datapipeline/paths.py`，因子路径维护在 `src/factors/paths.py`，Notebook 不依赖启动目录。

## 操作手册

- [数据库构建与更新指南](docs/database_workflow.md)
- [因子构建与回测指南](docs/factor_workflow.md)

## 数据流

1. `notebooks/datapipeline/CSV_First_Data_Pipeline.ipynb`
   - 合并 `data/csv/eco_calendar/` 中的月度原始 Excel，生成 filtered Excel 与 CSV。
   - 首次建库使用 `data/csv/T_mindf.csv`，并把日线/分钟线按合约保存为 CSV。
   - 日常增量更新回溯日线 5 个交易日、分钟线 2 个交易日，先更新 CSV，再事务化同步 DuckDB。
   - 每次写库前备份 DuckDB，并在 `logs/datapipeline/` 保存逐合约日志。
   - 同目录的 `CSV_First_Data_Pipeline.py` 提供等价命令行入口，可用 `--mode` 选择任务。
2. `notebooks/datapipeline/Compare_DuckDB_vs_T_mindf.ipynb`
   - 审计 DuckDB 与历史分钟 CSV 的差异。
   - 报告写入 `reports/data_pipeline/`。
3. `notebooks/factors/Data_Input_and_Quality.ipynb`
   - 从 DuckDB 或手工文件读取数据，完成质量控制与复权。
   - 中间表写入该因子目录下的 `working/`。
4. `python src/factors/cicc_close_session_reverse/refresh_roll_monitor.py`
   - 使用 `sql/roll_migration_monitor.sql` 刷新并校验 DuckDB 移仓监控视图。
   - 刷新日志和质量摘要写入 `logs/factors/`。
   - `notebooks/factors/Roll_Migration_Monitor.ipynb` 仅用于只读查看监控结果。
5. `python src/factors/cicc_close_session_reverse/build_signals.py`
   - 使用 Python 程序构建九类信号，输出 `output/final_9_category_signals.xlsx`。
   - 构建日志和运行摘要写入 `logs/factors/`。
6. `notebooks/factors/Backtest_Final_9_Category_Factors.ipynb`
   - 读取信号和验证数据，完成回测并输出结果。

## 目录

```text
TreasuryFutures/
├─ data/
│  ├─ input/                     # 外部输入与历史数据
│  ├─ csv/                       # 手工原始输入与 DuckDB 可审计标准数据源
│  │  ├─ eco_calendar/           # 月度经济日历原始 Excel
│  │  ├─ T_mindf.csv             # 历史分钟初始化与补丁输入
│  │  ├─ reference/              # 经济日历、合约目录、主力映射
│  │  ├─ daily/                  # 每合约一个日线 CSV
│  │  └─ minute/                 # 每合约一个分钟线 CSV
│  ├─ database/                  # DuckDB 数据库
│  └─ backups/duckdb/            # 写库前的时间戳备份
├─ notebooks/
│  ├─ datapipeline/              # 建库与数据审计
│  └─ factors/                   # 数据检查、移仓监控与回测 Notebook
├─ logs/
│  ├─ datapipeline/              # 数据管线运行日志
│  └─ factors/                   # 因子构建与回测日志
├─ factors/cicc_close_session_reverse/
│  ├─ sql/                       # 移仓监控 SQL
│  ├─ working/                   # 可再生成的中间数据
│  └─ output/                    # 因子与回测结果
├─ reports/data_pipeline/        # 数据管线审计报告
└─ src/
   ├─ datapipeline/              # 数据采集、CSV 与 DuckDB 管线
   └─ factors/                   # 因子构建与回测实现
```

主要实现与说明文件：

- `src/datapipeline/data_pipeline.py`：DuckDB 建表、Wind 更新、主连与复权基础函数。
- `src/datapipeline/pipeline_manager.py`：全量初始化、日常增量更新和补丁任务的统一编排入口。
- `src/datapipeline/eco_calendar_pipeline.py`：合并原始 Excel，生成过滤后的经济日历 Excel 与 CSV。
- `src/datapipeline/reference_data_pipeline.py`：更新合约目录与主力映射 CSV。
- `src/datapipeline/daily_data_pipeline.py`：按合约更新日线 CSV，并保留交易日重叠窗口。
- `src/datapipeline/minute_data_pipeline.py`：按合约更新分钟线 CSV，处理 `T_mindf.csv` 首次导入与历史补丁。
- `src/datapipeline/duckdb_sync_pipeline.py`：备份 DuckDB，并把变更的 CSV 区间增量同步入库。
- `src/datapipeline/pipeline_io.py`：CSV 原子写入、区间覆盖、运行日志等公共基础能力。
- `src/factors/cicc_close_session_reverse/refresh_roll_monitor.py`：刷新移仓监控视图、执行质量检查并记录日志。
- `src/factors/cicc_close_session_reverse/signal_builder.py`：九类因子的输入校验、特征计算和信号公式。
- `src/factors/cicc_close_session_reverse/build_signals.py`：信号构建、质量检查、Excel 输出与日志入口。
- `src/factors/cicc_close_session_reverse/backtest.py`：回测、统计、图表和工作簿导出函数。
- `src/datapipeline/paths.py`、`src/factors/paths.py`：两条工作流各自的路径配置入口。
- `data/database/README.md`：DuckDB 的数据来源、表结构、字段口径、质量提示和查询示例。

## 使用方式

从项目根目录启动 Jupyter：

```powershell
jupyter lab
```

Notebook 会自动定位项目根目录并加载 `src/`，无需依赖当前工作目录。若希望在其他 Python 程序中直接导入，也可以在项目根目录执行：

```powershell
python -m pip install -e .
```

`WindPy` 由 Wind 终端环境提供，不通过本项目的 `pyproject.toml` 安装。

因子信号构建：

```powershell
python src/factors/cicc_close_session_reverse/refresh_roll_monitor.py
python src/factors/cicc_close_session_reverse/build_signals.py
```

成功后再打开 `notebooks/factors/Backtest_Final_9_Category_Factors.ipynb` 进行筛选、回测和图表分析。
