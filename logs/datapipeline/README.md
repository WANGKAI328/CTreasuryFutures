# 数据管线日志

每次全量、增量或补丁运行会创建独立的 `<run_id>/` 目录，其中包括：

- `pipeline.log`：便于人工阅读；
- `events.jsonl`：结构化事件；
- 逐合约更新摘要 CSV；
- `run_summary.json`：运行结果、备份路径和统计。
