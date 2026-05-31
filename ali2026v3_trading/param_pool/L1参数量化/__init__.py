# R13-P1-DEAD-05修复: L1参数量化模块死代码标记
# R32-P1-01修复: 更新external_validation_pipeline标记(已被task_scheduler.py生产调用)
# 以下模块仅被test文件引用，无生产调用方，属于死代码：
# - v7_meta_audit_v2.py (仅advanced_validation.py和test引用)
# - performance_tier_manager.py (仅test引用)
# - soft_constrained_optimizer.py (仅test引用)
# - triple_truth_anchor.py (仅test引用)
# 生产活跃模块：
# - bayesian_shrinkage_life_estimator.py (被task_scheduler.py使用)
# - duckdb_tick_storage.py (被数据层使用)
# - external_validation_pipeline.py (被task_scheduler.py:7326生产调用, R32-P1-01修正)
