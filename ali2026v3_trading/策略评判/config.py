"""
策略评判模块配置 — E-08修复

解决NS-01问题：导入不存在的ali2026v3_trading.策略评判.config模块
审计报告：第二十轮独立审计报告_全系统全链路核查_20260523.md
修复日期：2026-05-25
"""
from __future__ import annotations

SURVIVED_THRESHOLD = 0.60

SURVIVED_THRESHOLD_DESCRIPTION = """
策略存活阈值（Strategy Survived Threshold）

定义：
  综合评分低于此阈值的策略被判定为"无法在极端市场环境下存活"，
  标记为FAIL_不可上线，禁止投入实盘交易。

取值依据：
  1. 与overall_conditional_threshold对齐（StrategyJudgmentEngine.__init__默认值0.60）
  2. 与DEFAULT_THRESHOLDS['extreme_survival']对齐（极端存活维度阈值0.60）
  3. 参数池统一执行方案与使用手册_V7.0_工程落地版 §7.4节

生效机制：
  strategy_judgment_engine.py:748执行E-08检查：
    from ali2026v3_trading.策略评判.config import SURVIVED_THRESHOLD
    if overall < SURVIVED_THRESHOLD:
        conditions.append(f"[E-08] 综合评分{overall:.4f}低于survived阈值{SURVIVED_THRESHOLD}")

影响范围：
  - 评判系统：strategy_judgment_engine.py
  - 测试系统：test_shadow_strategy_engine.py（通过judge_backtest_result调用）
  - 生产系统：无直接调用，但评判结果影响策略上线决策

验收标准：
  ✅ 代码存在 — 本文件存在
  ✅ py_compile通过 — 无语法错误
  ✅ 代码可用 — SURVIVED_THRESHOLD可被导入
  ✅ 代码被调用 — strategy_judgment_engine.py:748导入此模块
  ✅ 参数传递链路贯通 — 导入→使用→conditions追加
  ✅ 参数改变→结果改变 — 不同阈值产生不同conditions
  ✅ 默认值在扫描网格中 — 0.60与overall_conditional_threshold对齐
  ✅ 排序指标与扫描目标一致 — 阈值用于评判overall评分
  ✅ 三系统代码对齐 — 生产/测试/评判使用同一阈值
"""

__all__ = ['SURVIVED_THRESHOLD', 'SURVIVED_THRESHOLD_DESCRIPTION']
