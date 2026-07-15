# Treasury Futures

项目按“行情数据管线”和“CICC 尾盘反转因子”两条职责拆分。所有路径统一维护在 `src/treasury_futures/paths.py`，Notebook 不再依赖启动目录。

## 数据流

1. `notebooks/data_pipeline/01_TreasuryFutures.ipynb`
   - 使用 Wind 与 `data/input/T_mindf.csv`。
   - 全量或增量更新 `data/database/treasury_futures.duckdb`。
2. `notebooks/data_pipeline/02_Compare_DuckDB_vs_T_mindf.ipynb`
   - 审计 DuckDB 与历史分钟 CSV 的差异。
   - 报告写入 `reports/data_pipeline/`。
3. `factors/cicc_close_session_reverse/notebooks/01_Data_Input_and_Quality.ipynb`
   - 从 DuckDB 或手工文件读取数据，完成质量控制与复权。
   - 中间表写入该因子目录下的 `working/`。
4. `02_Roll_Migration_Monitor.ipynb`
   - 使用 `sql/roll_migration_monitor.sql` 刷新 DuckDB 移仓监控视图。
5. `03_Build_Final_9_Category_Factors.ipynb`
   - 构建九类信号，输出到该因子目录下的 `output/`。
6. `04_Backtest_Final_9_Category_Factors.ipynb`
   - 读取信号和验证数据，完成回测并输出结果。

## 目录

```text
TreasuryFutures/
├─ data/
│  ├─ input/                     # 外部输入与历史数据
│  └─ database/                  # DuckDB 数据库
├─ notebooks/data_pipeline/      # 建库与数据审计
├─ factors/cicc_close_session_reverse/
│  ├─ notebooks/                 # 01→04 顺序执行
│  ├─ sql/                       # 移仓监控 SQL
│  ├─ working/                   # 可再生成的中间数据
│  └─ output/                    # 因子与回测结果
├─ reports/data_pipeline/        # 数据管线审计报告
└─ src/treasury_futures/         # 可复用 Python 实现
```

主要实现与说明文件：

- `src/treasury_futures/data_pipeline.py`：原 `TreasuryFutures.ipynb` 中的建库、Wind 更新、主连与复权函数。
- `src/treasury_futures/factors/cicc_close_session_reverse/build.py`：九类因子的输入校验、特征计算和信号构建函数。
- `src/treasury_futures/factors/cicc_close_session_reverse/backtest.py`：回测、统计、图表和工作簿导出函数。
- `src/treasury_futures/paths.py`：数据库、输入、中间结果和输出目录的唯一配置入口。
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
