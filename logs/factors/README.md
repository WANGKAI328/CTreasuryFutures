# 因子运行日志

因子构建、质量检查和回测流程产生的运行日志统一保存在本目录。

运行 `python src/factors/cicc_close_session_reverse/refresh_roll_monitor.py` 后，
会创建 `refresh_roll_monitor_<timestamp>_<id>/`，其中包括：

- `refresh_roll_monitor.log`：刷新过程、数据不完整提示和状态分布；
- `run_summary.json`：视图日期范围、最新完整日期、屏蔽数量和运行状态。

运行 `python src/factors/cicc_close_session_reverse/build_signals.py` 后，会创建
`build_signals_<timestamp>_<id>/`，其中包括：

- `build_signals.log`：便于人工阅读的执行日志；
- `run_summary.json`：输出路径、信号数量、移仓屏蔽数量和运行状态。
