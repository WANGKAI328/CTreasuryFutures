# DuckDB 备份

CSV-first 管线在每次修改 `treasury_futures.duckdb` 前，将数据库复制到 `duckdb/` 并验证备份可以只读打开。备份文件带运行时间和用途标签，当前不自动删除。
