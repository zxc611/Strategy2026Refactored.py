# config/ — 配置文件目录

本目录包含ali2026v3_trading量化交易系统的核心配置文件。

## 文件说明

| 文件 | 格式 | 功能 | 对应模块 |
|------|------|------|----------|
| params_default.json | JSON | 全局默认参数配置 | config_params.py |
| cascade_config.yaml | YAML | 瀑布评判三层阈值配置(L1盈亏比/L2三角验证/L3小资金约束) | evaluation/cascade_judge.py |
| judgment_scoring_config.yaml | YAML | 策略评判评分权重配置(盈利能力/边际递减/权重归一化) | 策略评判/strategy_judgment_engine.py |
| cascade_threshold_grid.yaml | YAML | 瀑布评判阈值扫描网格(参数优化用) | 参数池/l2_optimizer.py |

## 参数三源对齐

所有配置参数需保持三源一致：YAML配置值 = Code默认值 = V7.0手册值。
参见`参数池/tvf_params.yaml`中的TVF参数和`config_params.py`中的CENTRALIZED_DEFAULTS。

## 热加载

部分参数支持运行时热加载（通过config_params.py的hot_reload机制）。
不支持热加载的参数列在`_HOT_RELOAD_UNSUPPORTED_KEYS`中，变更后需重启。
可通过`get_hot_reload_status()`查询当前热加载范围。
