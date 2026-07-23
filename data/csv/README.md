# 标准 CSV 数据层

本目录同时保存手工原始输入和 DuckDB 的可审计标准数据源，由
`CSV_First_Data_Pipeline.ipynb` 或同名 `.py` 入口维护：

- `eco_calendar/`：手工下载的月度经济日历原始 Excel；
- `T_mindf.csv`：首次建库和历史分钟补丁使用的原始分钟数据；
- `reference/eco_calendar_filtered.csv`：经济日历；
- `reference/contracts.csv`：合约目录；
- `reference/main_contract_mapping.csv`：逐日主力映射；
- `daily/<wind_code>.csv`：每合约一个日线文件；
- `minute/<wind_code>.csv`：每合约一个分钟线文件。

文件采用 UTF-8 with BOM，先写临时文件并回读校验，成功后才原子替换正式文件。不要手工删除仍被 DuckDB 使用的 CSV；历史修复请使用 Notebook 中的补丁入口。
