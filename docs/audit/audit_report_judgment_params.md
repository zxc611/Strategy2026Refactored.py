# 策略评判引擎 & 参数池生产模块排查报告

> 生成时间: 2026-05-18
> 排查范围: 8个模块（策略评判3个 + 参数池5个）
> 排查维度: 方法完整性 / 集成完备性 / 方法重复冲突 / 魔法数字参数化

---

## 模块1: strategy_judgment_engine.py

### 模块总览
- **文件路径**: `ali2026v3_trading/策略评判/strategy_judgment_engine.py`
- **行数**: ~1312行
- **类数**: 5个（JudgmentVerdict, CapitalScale, _JudgmentDimension, JudgmentReport, StrategyJudgmentEngine）
- **方法数**: 约22个方法/函数

### 方法完整性检查结果

| 方法名 | 类型 | 状态 | 说明 |
|--------|------|------|------|
| `__init__` | 构造 | 完整 | 支持thresholds/weights/capital_scale等参数 |
| `judge` | 核心 | 完整 | 12维度评判主入口 |
| `_diminishing_return_score` | 静态 | 完整 | 边际递减评分函数 |
| `_judge_profitability` | 私有 | 完整 | 盈利能力评判 |
| `_judge_profitability_by_scale` | 私有 | 完整 | 按资金规模评判 |
| `_resolve_scale_config` | 私有 | 完整 | 解析资金规模配置 |
| `_judge_behavior_consistency` | 私有 | 完整 | 行为一致性评判 |
| `_judge_process_explainability` | 私有 | 完整 | 过程可解释性评判 |
| `_judge_statistical_significance` | 私有 | 完整 | 统计显著性评判 |
| `_judge_risk_budget_compliance` | 私有 | 完整 | 风险预算遵守评判 |
| `_judge_extreme_survival` | 私有 | 完整 | 极端生存能力评判 |
| `_judge_cross_instrument_consistency` | 私有 | 完整 | 跨品种一致性评判 |
| `_judge_prediction_calibration` | 私有 | 完整 | 预测能力校准评判 |
| `_judge_parameter_stability` | 私有 | 完整 | 参数稳定性评判 |
| `_judge_return_source_diversification` | 私有 | 完整 | 收益来源分散度评判 |
| `_judge_drawdown_recovery` | 私有 | 完整 | 回撤恢复时间评判 |
| `_judge_cross_strategy_correlation` | 私有 | 完整 | 策略间相关性评判 |
| `_run_deep_validations` | 私有 | 完整 | 深度验证调用（动态导入） |
| `_determine_verdict` | 私有 | 完整 | 裁决判定逻辑 |
| `_generate_recommendations` | 私有 | 完整 | 生成建议 |
| `batch_judge` | 公有 | 完整 | 批量评判 |

**空方法/占位符**: 无

### 集成完备性

**被谁调用**:
- `backtest_integration_hooks.py` 导入并实例化 `StrategyJudgmentEngine`
- `parameter_pool_adapter.py` 导入并调用 `StrategyJudgmentEngine`
- `usage_example.py` 示例代码中使用
- `__init__.py` 导出

**调用了谁**:
- `strategy_behavior_diagnosis.py` (StrategyBehaviorDiagnosis, DiagnosisReport等)
- `resonance_turning_point_marker.py`
- `market_snapshot_collector.py`
- 动态导入 `参数池` 下的多个模块（statistical_validation, advanced_validation, l2_optimizer, test_design_suite, task_scheduler）

### 方法重复/冲突
- 无重复方法名
- `judge()` 方法名在 Python 内置中存在，但此处为类方法，无冲突

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 210-221 | 0.50, 0.70, 0.65... | DEFAULT_THRESHOLDS | 已参数化为类构造参数，可接受 |
| 224-237 | 0.10, 0.20, 0.07... | DEFAULT_WEIGHTS | 已参数化为类构造参数，可接受 |
| 371 | 0.75 | overall_pass_threshold默认 | 构造参数，可接受 |
| 372 | 0.60 | overall_conditional_threshold默认 | 构造参数，可接受 |
| 373 | 30 | min_samples默认 | 构造参数，可接受 |
| 374 | 3 | min_instruments默认 | 构造参数，可接受 |
| 375 | 100 | min_signals_for_stability默认 | 构造参数，可接受 |
| 376 | 3 | insufficient_evidence_missing_count默认 | 构造参数，可接受 |
| 377 | 24.0 | max_recovery_hours默认 | 构造参数，可接受 |
| 506 | [(1.0, 0.50), (2.0, 0.80), (3.0, 1.00)] | 默认breakpoints | 已参数化为函数参数，可接受 |
| 539 | 0.6 | win_rate满分阈值 | 应参数化（影响策略逻辑） |
| 617 | 1.0, 0.50 | sharpe/calmar评分断点 | 已参数化为scale_cfg，可接受 |
| 678 | 3.0 | avg_chain_length满分阈值 | 应参数化（影响策略逻辑） |
| 679 | 10.0 | unique_rules_triggered满分阈值 | 应参数化（影响策略逻辑） |
| 717 | 0.5 | is_auditable阈值乘数 | 应参数化（影响策略逻辑） |
| 766 | 30.0, 20.0 | max_dd惩罚阈值 | 应参数化（影响策略逻辑） |
| 789 | 1.0, 1e-8 | CV计算常数 | 技术常数，可保留 |
| 829 | 3.0, 5.0 | kurtosis/skewness惩罚分母 | 应参数化（影响策略逻辑） |
| 916 | 1.0, 0.30 | normal_ok计算常数 | 应参数化（影响策略逻辑） |
| 989 | 0.65, 3, 1.5 | L2Optimizer硬编码参数 | 应参数化（影响策略逻辑） |
| 1020 | 0.05 | BH校正p值阈值 | 应参数化（影响策略逻辑） |

### 模块排查结论
- **状态**: 生产就绪，核心逻辑完整
- **风险点**: 
  1. `_run_deep_validations` 中大量硬编码参数（如L2Optimizer参数、BH阈值0.05等）
  2. 动态导入异常处理过于宽泛（`except Exception: pass`），可能静默失败
  3. `TwelveStrategyRunner.run()` 调用使用 `hasattr` 检查，若方法不存在则跳过，缺乏明确错误提示

---

## 模块2: backtest_integration_hooks.py

### 模块总览
- **文件路径**: `ali2026v3_trading/策略评判/backtest_integration_hooks.py`
- **行数**: ~218行
- **类数**: 2个（HookConfig, BacktestIntegrationHooks）
- **方法数**: 约8个

### 方法完整性检查结果

| 方法名 | 类型 | 状态 | 说明 |
|--------|------|------|------|
| `__init__` | 构造 | 完整 | 初始化diagnoser和judgment_engine |
| `on_backtest_complete` | 公有 | 完整 | 回测完成回调 |
| `on_new_trades` | 公有 | 完整 | 新交易回调 |
| `on_market_snapshot` | 公有 | 完整 | 市场快照回调 |
| `get_latest_report` | 公有 | 完整 | 获取最新报告 |
| `get_latest_verdict` | 公有 | 完整 | 获取最新裁决 |
| `reset` | 公有 | 完整 | 重置状态 |

**空方法/占位符**: 无

### 集成完备性

**被谁调用**:
- `usage_example.py` 中使用
- `__init__.py` 导出

**调用了谁**:
- `strategy_behavior_diagnosis.py` (StrategyBehaviorDiagnosis)
- `strategy_judgment_engine.py` (StrategyJudgmentEngine)

### 方法重复/冲突
- 无重复方法名

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 75 | 100 | max_history默认 | 构造参数，可接受 |
| 78 | 0.70 | behavior_threshold默认 | 构造参数，可接受 |
| 81 | medium | capital_scale默认 | 枚举值，可接受 |

### 模块排查结论
- **状态**: 生产就绪，集成层完整
- **风险点**: 
  1. `on_new_trades` 中 `latest_trades` 列表可能无限增长（未设置上限）
  2. 异常处理使用裸 `except Exception`，可能吞掉重要错误

---

## 模块3: strategy_behavior_diagnosis.py

### 模块总览
- **文件路径**: `ali2026v3_trading/策略评判/strategy_behavior_diagnosis.py`
- **行数**: ~325行
- **类数**: 5个（DiagnosisSeverity, BehaviorConsistencyScore, DiagnosisDimension, DiagnosisReport, StrategyBehaviorDiagnosis）
- **方法数**: 约12个

### 方法完整性检查结果

| 方法名 | 类型 | 状态 | 说明 |
|--------|------|------|------|
| `__init__` | 构造 | 完整 | 初始化参数 |
| `diagnose` | 核心 | 完整 | 行为诊断主入口 |
| `_diagnose_signal_decay` | 私有 | 完整 | 信号衰减一致性诊断 |
| `_diagnose_position_smoothness` | 私有 | 完整 | 仓位平滑度诊断 |
| `_diagnose_greeks_rationality` | 私有 | 完整 | Greeks合理性诊断 |
| `_diagnose_pnl_path_quality` | 私有 | 完整 | 盈亏路径质量诊断 |
| `_compute_overall` | 私有 | 完整 | 计算综合得分 |
| `_generate_recommendations` | 私有 | 完整 | 生成建议 |
| `to_dict` | 公有 | 完整 | 序列化为字典 |
| `to_json` | 公有 | 完整 | 序列化为JSON |
| `save` | 公有 | 完整 | 保存到文件 |

**空方法/占位符**: 无

### 集成完备性

**被谁调用**:
- `strategy_judgment_engine.py` 导入并使用
- `backtest_integration_hooks.py` 导入并使用
- `usage_example.py` 中使用
- `__init__.py` 导出

**调用了谁**:
- 无外部模块调用（纯自包含逻辑）

### 方法重复/冲突
- 无重复方法名

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 78 | 0.70 | default_threshold | 构造参数，可接受 |
| 79 | 50 | min_samples | 构造参数，可接受 |
| 80 | 0.30 | min_confidence | 构造参数，可接受 |
| 81 | 0.10 | outlier_z_threshold | 构造参数，可接受 |
| 82 | 0.05 | structural_change_threshold | 构造参数，可接受 |
| 131 | 0.95 | 信号衰减权重 | 已参数化，可接受 |
| 132 | 0.05 | 信号衰减权重 | 已参数化，可接受 |
| 150 | 0.90 | 仓位平滑度权重 | 已参数化，可接受 |
| 151 | 0.10 | 仓位平滑度权重 | 已参数化，可接受 |
| 169 | 0.85 | Greeks权重 | 已参数化，可接受 |
| 170 | 0.15 | Greeks权重 | 已参数化，可接受 |
| 189 | 0.80 | PnL路径权重 | 已参数化，可接受 |
| 190 | 0.20 | PnL路径权重 | 已参数化，可接受 |
| 210 | 0.25 | 综合得分权重 | 已参数化，可接受 |
| 211 | 0.25 | 综合得分权重 | 已参数化，可接受 |
| 212 | 0.25 | 综合得分权重 | 已参数化，可接受 |
| 213 | 0.25 | 综合得分权重 | 已参数化，可接受 |

### 模块排查结论
- **状态**: 生产就绪，诊断逻辑完整
- **风险点**: 
  1. `diagnose()` 中 `trades` 为空时返回默认报告，可能导致误判
  2. 部分诊断维度（如greeks_rationality）在输入数据缺失时返回0分，可能误报

---

## 模块4: advanced_validation.py

### 模块总览
- **文件路径**: `ali2026v3_trading/参数池/advanced_validation.py`
- **行数**: ~430行
- **类数**: 12个（SurvivalAnalyzer, MultipleComparisonCorrector, WalkForwardValidator, DeepValidationSuite, OtherStateDefenseQuantifier, ParamTierManager, CrossPeriodOverlapValidator, ShadowParamIndependenceValidator, DifferentiatedAlphaChecker, ReverseStrategyValidator, OrderFlowFilterValidator, StateSwitchPositionPolicy）
- **方法数**: 约25个

### 方法完整性检查结果

| 方法名 | 类型 | 状态 | 说明 |
|--------|------|------|------|
| `SurvivalAnalyzer.analyze` | 公有 | 完整 | 生存分析 |
| `MultipleComparisonCorrector.bonferroni` | 静态 | 完整 | Bonferroni校正 |
| `MultipleComparisonCorrector.benjamini_hochberg` | 静态 | 完整 | BH校正 |
| `WalkForwardValidator.__init__` | 构造 | 完整 | 初始化 |
| `WalkForwardValidator.validate` | 公有 | 完整 | Walk-forward验证 |
| `DeepValidationSuite.validate_regime_robustness` | 公有 | 空实现 | 返回固定True |
| `DeepValidationSuite.validate_cross_strategy_correlation` | 公有 | 空实现 | 返回固定True |
| `DeepValidationSuite.validate_hft_temporal_robustness` | 公有 | 空实现 | 返回固定True |
| `DeepValidationSuite.validate_market_friendliness_baseline` | 公有 | 空实现 | 返回固定True |
| `DeepValidationSuite.validate_logic_transferability` | 公有 | 空实现 | 返回固定True |
| `DeepValidationSuite.validate_liquidity_stress` | 公有 | 空实现 | 返回固定True |
| `DeepValidationSuite.validate_doomed_tests` | 公有 | 空实现 | 返回固定True |
| `DeepValidationSuite.run_full_validation` | 公有 | 完整 | 调用上述空方法 |
| `OtherStateDefenseQuantifier.quantify` | 公有 | 完整 | 防御量化 |
| `ParamTierManager.get_tier` | 公有 | 完整 | 获取参数层级 |
| `ParamTierManager.get_params_for_tier` | 公有 | 完整 | 获取层级参数 |
| `ParamTierManager.should_calibrate` | 公有 | 完整 | 判断是否需要校准 |
| `generate_hft_fidelity_warning` | 函数 | 完整 | HFT保真度警告 |
| `CrossPeriodOverlapValidator.validate` | 公有 | 完整 | 跨期重叠验证 |
| `ShadowParamIndependenceValidator.validate` | 公有 | 完整 | 影子参数独立验证 |
| `DifferentiatedAlphaChecker.check` | 公有 | 完整 | 差异化Alpha检查 |
| `ReverseStrategyValidator.validate` | 公有 | 完整 | 反向策略验证 |
| `OrderFlowFilterValidator.validate_filter_effectiveness` | 公有 | 完整 | 过滤有效性 |
| `OrderFlowFilterValidator.validate_false_signal_injection` | 公有 | 完整 | 假信号注入 |
| `StateSwitchPositionPolicy.apply` | 公有 | 完整 | 状态切换持仓策略 |

**空方法/占位符**: 
- `DeepValidationSuite` 中6个 `validate_*` 方法均为空实现（返回 `{"passed": True, ...}`）
- 这些空方法在 `task_scheduler.py` 中有完整的同名函数实现

### 集成完备性

**被谁调用**:
- `strategy_judgment_engine.py` 动态导入并调用
- `task_scheduler.py` 未直接导入此模块

**调用了谁**:
- 无外部模块调用

### 方法重复/冲突

**严重冲突**: 
- `advanced_validation.py` 中的 `DeepValidationSuite` 类有6个空方法，与 `task_scheduler.py` 中同名全局函数冲突：
  - `validate_regime_robustness`
  - `validate_cross_strategy_correlation`
  - `validate_hft_temporal_robustness`
  - `validate_market_friendliness_baseline`
  - `validate_logic_transferability`
  - `validate_liquidity_stress`
  - `validate_doomed_tests`

- `strategy_judgment_engine.py` 中调用的是 `DeepValidationSuite().run_deep_validation({})`，但实际生产代码中 `task_scheduler.py` 的同名函数才是完整实现
- `l2_optimizer.py` 中的 `evaluate_state_accuracy`, `check_l2_statistical_power`, `check_l2_conflict`, `compute_alpha_confidence_interval` 也在 `task_scheduler.py` 中有同名全局函数

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 26 | [0.5, 1.0, 1.5, 2.0, 2.5, 3.0] | 默认tp_ratios | 应参数化（影响策略逻辑） |
| 62 | 1.0 | bonferroni上限 | 数学常数，可保留 |
| 95 | 5, 0.7, 20.0, 2 | WalkForward默认参数 | 构造参数，可接受 |
| 144 | 2 | consecutive_decline阈值 | 应参数化（影响策略逻辑） |
| 151 | 0.5 | sharpe_range阈值 | 应参数化（影响策略逻辑） |
| 185 | 3 | n_regimes默认 | 方法参数，可接受 |
| 191 | 0.6 | correlation_threshold | 方法参数，可接受 |
| 198 | [0.01, 0.05, 0.1] | 默认drop_probs | 应参数化（影响策略逻辑） |
| 200 | [1.0, 3.0, 5.0] | 默认delay_lambdas | 应参数化（影响策略逻辑） |
| 204 | 100 | n_random默认 | 方法参数，可接受 |
| 214 | [1, 5, 10, 20, 50] | 默认slippage_multipliers | 应参数化（影响策略逻辑） |
| 218 | 10 | n_shuffle默认 | 方法参数，可接受 |
| 262 | 0.60 | min_overlap默认 | 方法参数，可接受 |
| 341 | 0.20 | min_diff_pct默认 | 方法参数，可接受 |
| 356 | 0.5, 0.5, 0.3, 0.4 | ALPHA_THRESHOLDS | 应参数化（影响策略逻辑） |
| 404 | 0.05 | max_trigger_rate默认 | 方法参数，可接受 |

### 模块排查结论
- **状态**: 部分生产就绪，存在严重空方法问题
- **风险点**: 
  1. `DeepValidationSuite` 6个验证方法为空实现，实际逻辑在 `task_scheduler.py` 中
  2. 方法名冲突可能导致调用混淆（类方法 vs 全局函数）
  3. `validate_doomed_tests` 在 `task_scheduler.py` 中有完整实现，但 `advanced_validation.py` 中为空

---

## 模块5: statistical_validation.py

### 模块总览
- **文件路径**: `ali2026v3_trading/参数池/statistical_validation.py`
- **行数**: ~140行
- **类数**: 2个（CounterfactualValidator, MonteCarloBankruptcyValidator）
- **方法数**: 4个

### 方法完整性检查结果

| 方法名 | 类型 | 状态 | 说明 |
|--------|------|------|------|
| `CounterfactualValidator.__init__` | 构造 | 完整 | 初始化 |
| `CounterfactualValidator.validate` | 公有 | 完整 | 反事实验证 |
| `MonteCarloBankruptcyValidator.__init__` | 构造 | 完整 | 初始化 |
| `MonteCarloBankruptcyValidator.validate` | 公有 | 完整 | 蒙特卡洛破产验证 |

**空方法/占位符**: 无

### 集成完备性

**被谁调用**:
- `strategy_judgment_engine.py` 动态导入并调用

**调用了谁**:
- 无外部模块调用

### 方法重复/冲突
- 无重复方法名

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 22 | 1000, 0.95, 0.40 | Counterfactual默认参数 | 构造参数，可接受 |
| 49 | 95 | 百分位阈值 | 由confidence_level计算，可接受 |
| 87 | 1000, 5.0, 0.99 | MonteCarlo默认参数 | 构造参数，可接受 |
| 94 | 252 | 年交易日数 | 应参数化（影响策略逻辑） |
| 113 | 10000 | slippage_bps分母 | 单位换算常数，可保留 |

### 模块排查结论
- **状态**: 生产就绪，统计验证逻辑完整
- **风险点**: 
  1. `MonteCarloBankruptcyValidator.validate` 中 `n_trades_per_year=252` 为硬编码，不同市场可能不同
  2. `commission_per_trade` 和 `slippage_bps` 有默认值但可能不适用于所有品种

---

## 模块6: l2_optimizer.py

### 模块总览
- **文件路径**: `ali2026v3_trading/参数池/l2_optimizer.py`
- **行数**: ~242行
- **类数**: 2个（L2Optimizer, TwelveStrategyRunner）
- **方法数**: 9个

### 方法完整性检查结果

| 方法名 | 类型 | 状态 | 说明 |
|--------|------|------|------|
| `L2Optimizer.__init__` | 构造 | 完整 | 初始化 |
| `L2Optimizer.evaluate_state_accuracy` | 公有 | 完整 | 状态准确率评估 |
| `L2Optimizer.check_l2_statistical_power` | 公有 | 完整 | 统计功效检查 |
| `L2Optimizer.check_l2_conflict` | 公有 | 完整 | L2冲突检查 |
| `L2Optimizer.optimize_step1` | 公有 | 完整 | Step1优化 |
| `TwelveStrategyRunner.__init__` | 构造 | 完整 | 初始化 |
| `TwelveStrategyRunner.run_single_strategy` | 公有 | 完整 | 单策略运行 |
| `TwelveStrategyRunner.run_all_twelve` | 公有 | 完整 | 十二策略运行 |
| `TwelveStrategyRunner.compute_alpha_confidence_interval` | 公有 | 完整 | Alpha置信区间 |

**空方法/占位符**: 无

### 集成完备性

**被谁调用**:
- `strategy_judgment_engine.py` 动态导入并调用

**调用了谁**:
- 无外部模块调用

### 方法重复/冲突

**冲突**: 
- `evaluate_state_accuracy`, `check_l2_statistical_power`, `check_l2_conflict`, `compute_alpha_confidence_interval` 在 `task_scheduler.py` 中有同名全局函数
- `task_scheduler.py` 中的实现更完整（支持DataFrame输入、更多参数）

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 10-14 | 0.05, 0.30, 2, 7, 0.25, 1.0 | L2_PARAM_GRID | 应参数化（影响策略逻辑） |
| 32 | 100, 0.60, 0.10 | L2Optimizer默认参数 | 构造参数，可接受 |
| 41 | 5 | lookahead_bars默认 | 方法参数，可接受 |
| 42 | 0.65 | non_other_threshold默认 | 应参数化（影响策略逻辑） |
| 43 | 3 | state_confirm_bars默认 | 应参数化（影响策略逻辑） |
| 44 | 1.5 | logic_reversal_threshold默认 | 应参数化（影响策略逻辑） |
| 56 | 0.5 | 默认non_other_ratio | 应参数化（影响策略逻辑） |
| 59 | 1 - non_other_threshold | 状态判断阈值 | 已参数化，可接受 |
| 72 | 0.005 | other状态准确率阈值 | 应参数化（影响策略逻辑） |
| 153 | main, shadow_a, shadow_b | STRATEGY_VARIANTS | 枚举值，可接受 |
| 154 | s1_hft, s2_minute... | STRATEGY_IDS | 枚举值，可接受 |
| 156-161 | 0.5, 0.5, 0.3, 0.4 | ALPHA_THRESHOLDS | 应参数化（影响策略逻辑） |
| 194 | 0.8, 1.2 | shadow_a参数乘数 | 应参数化（影响策略逻辑） |
| 198 | 0.73, 0.6 | shadow_b参数乘数 | 应参数化（影响策略逻辑） |
| 233 | 0.95 | confidence默认 | 方法参数，可接受 |
| 235 | 2.0 | n_trades<2时的CI宽度 | 应参数化（影响策略逻辑） |
| 241 | 1.96, 2.0 | z值 | 统计常数，可保留 |

### 模块排查结论
- **状态**: 生产就绪，但存在方法名冲突
- **风险点**: 
  1. 与 `task_scheduler.py` 中同名全局函数冲突
  2. `TwelveStrategyRunner.run_single_strategy` 中 shadow_a/b 参数乘数硬编码
  3. `run_all_twelve` 方法名在代码中未找到实际调用（`strategy_judgment_engine.py` 中调用的是 `tsr.run({})`，但类中无 `run` 方法，只有 `run_all_twelve`）

---

## 模块7: test_design_suite.py

### 模块总览
- **文件路径**: `ali2026v3_trading/参数池/test_design_suite.py`
- **行数**: ~748行
- **类数**: 10个（TestDesignSuite, ConfigVersionControl, ExecutionChecklist, MultiGranularityBacktest, ExecutionPathValidator, ParameterTypeMutexChecker, KnownLimitations, MustFailTestSuite, ExecutionTimeline, MultiStrategyExecutionPathValidator）
- **方法数**: 约49个

### 方法完整性检查结果

| 方法名 | 类型 | 状态 | 说明 |
|--------|------|------|------|
| `TestDesignSuite.__init__` | 构造 | 完整 | 初始化 |
| `TestDesignSuite.run_all_tests` | 公有 | 完整 | 运行所有测试 |
| `TestDesignSuite._run_single_test` | 私有 | 空实现 | 返回固定 `{"passed": True}` |
| `ConfigVersionControl.__init__` | 构造 | 完整 | 初始化 |
| `ConfigVersionControl._load_history` | 私有 | 完整 | 加载历史 |
| `ConfigVersionControl.save_config` | 公有 | 完整 | 保存配置 |
| `ConfigVersionControl.rollback` | 公有 | 完整 | 回滚 |
| `ConfigVersionControl.rollback_to_previous` | 公有 | 完整 | 回滚到上一版本 |
| `ConfigVersionControl.get_history` | 公有 | 完整 | 获取历史 |
| `ConfigVersionControl.get_current_version` | 公有 | 完整 | 获取当前版本 |
| `ExecutionChecklist.__init__` | 构造 | 完整 | 初始化 |
| `ExecutionChecklist.check_data_integrity` | 公有 | 完整 | 数据完整性检查 |
| `ExecutionChecklist.check_parameter_validity` | 公有 | 完整 | 参数有效性检查 |
| `ExecutionChecklist.check_backtest_environment` | 公有 | 完整 | 回测环境检查 |
| `ExecutionChecklist.check_result_plausibility` | 公有 | 完整 | 结果合理性检查 |
| `ExecutionChecklist.run_full_checklist` | 公有 | 完整 | 运行完整检查清单 |
| `MultiGranularityBacktest.__init__` | 构造 | 完整 | 初始化 |
| `MultiGranularityBacktest.run_backtest_for_granularity` | 公有 | 完整 | 单粒度回测 |
| `MultiGranularityBacktest.run_all_granularities` | 公有 | 完整 | 全粒度回测 |
| `MultiGranularityBacktest.select_optimal_granularity` | 公有 | 完整 | 选择最优粒度 |
| `ExecutionPathValidator.__init__` | 构造 | 完整 | 初始化 |
| `ExecutionPathValidator.validate_step1` | 公有 | 完整 | Step1验证 |
| `ExecutionPathValidator.validate_step2` | 公有 | 完整 | Step2验证 |
| `ExecutionPathValidator.validate_step3` | 公有 | 完整 | Step3验证 |
| `ExecutionPathValidator.validate_all_steps` | 公有 | 完整 | 全步骤验证 |
| `ParameterTypeMutexChecker.__init__` | 构造 | 完整 | 初始化 |
| `ParameterTypeMutexChecker.check_types` | 公有 | 完整 | 类型检查 |
| `ParameterTypeMutexChecker.check_mutex` | 公有 | 完整 | 互斥检查 |
| `ParameterTypeMutexChecker.check_all` | 公有 | 完整 | 全部检查 |
| `KnownLimitations.__init__` | 构造 | 完整 | 初始化 |
| `KnownLimitations.get_limitations` | 公有 | 完整 | 获取限制 |
| `KnownLimitations.acknowledge` | 公有 | 完整 | 确认限制 |
| `KnownLimitations.check_all_acknowledged` | 公有 | 完整 | 检查全部确认 |
| `MustFailTestSuite.__init__` | 构造 | 完整 | 初始化 |
| `MustFailTestSuite.test_random_strategy` | 公有 | 完整 | 随机策略测试 |
| `MustFailTestSuite.test_zero_risk_ratio` | 公有 | 完整 | 零风险测试 |
| `MustFailTestSuite.test_inverted_tp_sl` | 公有 | 完整 | 反向止盈止损测试 |
| `MustFailTestSuite.test_extreme_overfitting` | 公有 | 完整 | 极端过拟合测试 |
| `MustFailTestSuite.test_stale_data` | 公有 | 完整 | 过期数据测试 |
| `MustFailTestSuite.run_all` | 公有 | 完整 | 运行全部测试 |
| `ExecutionTimeline.__init__` | 构造 | 完整 | 初始化 |
| `ExecutionTimeline.start_project` | 公有 | 完整 | 启动项目 |
| `ExecutionTimeline.get_timeline` | 公有 | 完整 | 获取时间线 |
| `ExecutionTimeline.get_total_hours` | 公有 | 完整 | 获取总时长 |
| `ExecutionTimeline.mark_milestone` | 公有 | 完整 | 标记里程碑 |
| `ExecutionTimeline.get_progress` | 公有 | 完整 | 获取进度 |
| `MultiStrategyExecutionPathValidator.__init__` | 构造 | 完整 | 初始化 |
| `MultiStrategyExecutionPathValidator.validate_strategy_path` | 公有 | 完整 | 验证策略路径 |
| `MultiStrategyExecutionPathValidator.validate_all_strategies` | 公有 | 完整 | 验证全部策略 |

**空方法/占位符**: 
- `TestDesignSuite._run_single_test` 为空实现（返回固定 `{"passed": True, "reason": "placeholder"}`）

### 集成完备性

**被谁调用**:
- `strategy_judgment_engine.py` 动态导入并调用

**调用了谁**:
- 无外部模块调用

### 方法重复/冲突
- 无重复方法名

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 14-99 | 大量测试名称字符串 | PHASE_*_TESTS | 配置常量，可接受 |
| 139 | True | _run_single_test返回值 | 空实现，需完善 |
| 164 | "system", "auto_save" | save_config默认值 | 可接受 |
| 243-249 | 0.5-5.0, 0.1-2.0... | 默认参数范围 | 应参数化（影响策略逻辑） |
| 276 | -2.0, 5.0 | sharpe范围 | 应参数化（影响策略逻辑） |
| 277 | 0 | profit_loss_ratio阈值 | 应参数化（影响策略逻辑） |
| 278 | 0, 1.0 | max_drawdown范围 | 应参数化（影响策略逻辑） |
| 302 | [1, 5, 15, 30, 60] | GRANULARITIES | 应参数化（影响策略逻辑） |
| 372 | 0.05 | counterfactual_p_value阈值 | 应参数化（影响策略逻辑） |
| 423-431 | 参数类型定义 | PARAM_TYPES | 配置常量，可接受 |
| 534 | 0.3 | random_strategy sharpe阈值 | 应参数化（影响策略逻辑） |
| 545 | 0.0 | zero_risk_ratio阈值 | 应参数化（影响策略逻辑） |
| 555 | 0.5, 1.0 | inverted_tp_sl默认值 | 方法参数，可接受 |
| 568 | 0.8 | overfitting degradation阈值 | 应参数化（影响策略逻辑） |
| 578 | 30 | stale_data天数阈值 | 应参数化（影响策略逻辑） |
| 610-619 | 8, 4, 24... | DEFAULT_TIMELINE时长 | 配置常量，可接受 |

### 模块排查结论
- **状态**: 大部分生产就绪，存在空方法
- **风险点**: 
  1. `TestDesignSuite._run_single_test` 为空实现，所有测试默认通过
  2. `ExecutionChecklist.check_parameter_validity` 中参数范围硬编码
  3. `MustFailTestSuite` 中阈值硬编码

---

## 模块8: task_scheduler.py

### 模块总览
- **文件路径**: `ali2026v3_trading/参数池/task_scheduler.py`
- **行数**: ~4890行
- **类数**: 3个（_BacktestPosition, _ClosedTrade, _BacktestState）
- **方法/函数数**: 约60个

### 方法完整性检查结果

| 方法/函数名 | 类型 | 状态 | 说明 |
|-------------|------|------|------|
| `validate_shadow_param_independence` | 函数 | 完整 | 影子参数独立性验证 |
| `_resolve_tp_sl` | 函数 | 完整 | 解析止盈止损 |
| `_resolve_time_stop` | 函数 | 完整 | 解析时间止损 |
| `_compute_lots_with_risk_budget` | 函数 | 完整 | 计算手数 |
| `_check_state_transition` | 函数 | 完整 | 状态转换检查 |
| `_compute_dynamic_slippage_bps` | 函数 | 完整 | 动态滑点计算 |
| `_check_logic_reversal` | 函数 | 完整 | 逻辑反转检查 |
| `_check_safety` | 函数 | 完整 | 安全检查 |
| `_try_open` | 函数 | 完整 | 尝试开仓 |
| `_check_two_stage_stop` | 函数 | 完整 | 两阶段止损检查 |
| `_check_positions` | 函数 | 完整 | 持仓检查 |
| `_compute_profit_loss_ratio_metrics` | 函数 | 完整 | 计算盈亏比指标 |
| `_reset_daily` | 函数 | 完整 | 日重置 |
| `run_backtest` | 函数 | 完整 | 主回测函数 |
| `run_backtest_box_extreme` | 函数 | 完整 | 箱体极值回测 |
| `run_backtest_box_spring` | 函数 | 完整 | 箱体弹簧回测 |
| `run_backtest_hft` | 函数 | 完整 | HFT回测 |
| `run_backtest_arbitrage` | 函数 | 完整 | 套利回测 |
| `run_backtest_market_making` | 函数 | 完整 | 做市回测 |
| `run_backtest_hft_with_disturbance` | 函数 | 完整 | HFT扰动回测 |
| `validate_hft_temporal_robustness` | 函数 | 完整 | HFT时序鲁棒性验证 |
| `validate_cross_strategy_correlation` | 函数 | 完整 | 跨策略相关性验证 |
| `validate_market_friendliness_baseline` | 函数 | 完整 | 市场友善度验证 |
| `validate_regime_robustness` | 函数 | 完整 | 机制鲁棒性验证 |
| `validate_liquidity_stress` | 函数 | 完整 | 流动性压力验证 |
| `validate_doomed_tests` | 函数 | 完整 | 注定失败测试 |
| `validate_logic_transferability` | 函数 | 完整 | 逻辑可迁移性验证 |
| `run_deep_validation_tiered` | 函数 | 完整 | 分级深度验证 |
| `run_deep_validation_suite` | 函数 | 完整 | 完整深度验证套件 |
| `check_l2_statistical_power` | 函数 | 完整 | L2统计功效检查 |
| `analyze_l2_sensitivity` | 函数 | 完整 | L2敏感性分析 |
| `compute_alpha_confidence_interval` | 函数 | 完整 | Alpha置信区间 |
| `evaluate_state_accuracy` | 函数 | 完整 | 状态准确率评估 |
| `optimize_l2_params_step1` | 函数 | 完整 | L2参数优化Step1 |
| `check_l2_conflict` | 函数 | 完整 | L2冲突检查 |
| `run_step2_smoke_test` | 函数 | 完整 | Step2冒烟测试 |
| `_ensure_results_table` | 函数 | 完整 | 确保结果表存在 |
| `_get_completed_task_ids` | 函数 | 完整 | 获取已完成任务ID |
| `_load_data_for_period` | 函数 | 完整 | 加载周期数据 |
| `_load_multiscale_data` | 函数 | 完整 | 加载多粒度数据 |
| `_resample_bars_runtime` | 函数 | 完整 | 运行时重采样 |
| `_interpolate_ticks_in_bar` | 函数 | 完整 | Bar内tick插值 |
| `run_backtest_multiscale` | 函数 | 完整 | 多粒度回测 |
| `run_backtest_hft_tick_fidelity` | 函数 | 完整 | HFT tick保真回测 |
| `run_kline_length_sweep` | 函数 | 完整 | K线长度扫描 |
| `run_kline_length_deep_sweep` | 函数 | 完整 | K线深度扫描 |
| `run_kline_cr_cross_sweep` | 函数 | 完整 | K线×CR交叉扫描 |
| `validate_kline_length_quality_gates` | 函数 | 完整 | K线质量门验证 |
| `_worker_init` | 函数 | 完整 | 工作进程初始化 |
| `_worker_task` | 函数 | 完整 | 工作进程任务 |
| `_insert_results` | 函数 | 完整 | 插入结果 |
| `_execute_round` | 函数 | 完整 | 执行一轮扫描 |
| `_run_final_checks` | 函数 | 完整 | 最终检查 |
| `_validate_params_via_params_service` | 函数 | 完整 | 参数服务验证 |
| `main_scheduler` | 函数 | 完整 | 主调度器 |
| `_infer_hmm_state_from_iv` | 函数 | 完整 | HMM状态推断 |
| `_infer_trend_scores_from_bar` | 函数 | 完整 | 趋势得分推断 |
| `run_backtest_with_cycle_resonance` | 函数 | 完整 | 周期共振回测 |
| `run_cycle_resonance_backtest_sweep` | 函数 | 完整 | 周期共振扫描 |
| `run_cr_params_sweep` | 函数 | 完整 | CR参数扫描 |

**空方法/占位符**: 无

### 集成完备性

**被谁调用**:
- `strategy_judgment_engine.py` 动态导入 `validate_doomed_tests`
- 自身 `main_scheduler` 为入口函数
- `run_backtest_*` 系列函数被多个验证函数调用

**调用了谁**:
- `cycle_resonance_module` (动态导入)
- `params_service` (动态导入)
- `ali2026v3_trading.mode_engine` (动态导入)
- `evaluation.cascade_judge` (动态导入)

### 方法重复/冲突

**严重冲突**:
- 以下函数与 `advanced_validation.py` 中 `DeepValidationSuite` 类方法同名：
  - `validate_regime_robustness`
  - `validate_cross_strategy_correlation`
  - `validate_hft_temporal_robustness`
  - `validate_market_friendliness_baseline`
  - `validate_logic_transferability`
  - `validate_liquidity_stress`
  - `validate_doomed_tests`

- 以下函数与 `l2_optimizer.py` 中 `L2Optimizer` / `TwelveStrategyRunner` 类方法同名：
  - `evaluate_state_accuracy`
  - `check_l2_statistical_power`
  - `check_l2_conflict`
  - `compute_alpha_confidence_interval`

- `strategy_judgment_engine.py` 中调用的是类方法（如 `L2Optimizer().evaluate_state_accuracy(...)`），但 `task_scheduler.py` 中的全局函数实现更完整

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 31 | 4, 2, 16 | MAX_WORKERS计算 | 应参数化（影响性能） |
| 32-34 | 日期范围 | TRAIN_START/TEST_START/TEST_END | 应参数化（影响策略逻辑） |
| 36 | 1_000_000.0 | INITIAL_EQUITY | 应参数化（影响策略逻辑） |
| 37 | 1.5 | COMMISSION_PER_LOT | 应参数化（影响策略逻辑） |
| 38 | 1.0 | SLIPPAGE_BPS | 应参数化（影响策略逻辑） |
| 40-52 | 大量pullback默认值 | PULLBACK_DEFAULTS | 应参数化（影响策略逻辑） |
| 64-71 | 大量参数网格值 | PARAM_GRID_ROUND1 | 应参数化（影响策略逻辑） |
| 75-97 | 大量参数默认值 | PARAM_DEFAULTS | 应参数化（影响策略逻辑） |
| 297-304 | 0.8, 0.87, 0.73... | REASON_MULTIPLIERS | 应参数化（影响策略逻辑） |
| 319 | 10 | ROUND1_TOP_K | 应参数化（影响策略逻辑） |
| 342 | 0.20 | validate_shadow_param_independence阈值 | 方法参数，可接受 |
| 410 | 100_000 | equity_curve maxlen | 技术常数，可保留 |
| 442 | 1.5, 0.5 | _resolve_tp_sl默认值 | 应参数化（影响策略逻辑） |
| 450 | 90.0 | _resolve_time_stop默认值 | 应参数化（影响策略逻辑） |
| 478 | 3, 10, 0.6, 0.4, 0.5, 0.75 | 手数调整阈值 | 应参数化（影响策略逻辑） |
| 493 | 0.4 | non_other_ratio_threshold默认 | 应参数化（影响策略逻辑） |
| 503 | 3 | state_confirm_bars默认 | 应参数化（影响策略逻辑） |
| 524 | 1.0 | _compute_dynamic_slippage_bps默认 | 应参数化（影响策略逻辑） |
| 543 | 0.5 | spread_bps乘数 | 应参数化（影响策略逻辑） |
| 565 | 1.5 | logic_reversal_threshold默认 | 应参数化（影响策略逻辑） |
| 567 | 0.3 | wrong_pct阈值 | 应参数化（影响策略逻辑） |
| 599 | 0.05 | daily_loss_hard_stop_pct默认 | 应参数化（影响策略逻辑） |
| 622 | 5, 100 | max_signals限制 | 应参数化（影响策略逻辑） |
| 634 | 0.3 | strength阈值 | 应参数化（影响策略逻辑） |
| 684-687 | 90.0, 0.002, 10, 0.0 | 两阶段止损参数 | 应参数化（影响策略逻辑） |
| 766 | 14, 55 | EOD时间 | 应参数化（影响策略逻辑） |
| 792 | 0.03 | circuit_breaker触发阈值 | 应参数化（影响策略逻辑） |
| 793 | 180.0 | circuit_breaker_pause_sec默认 | 应参数化（影响策略逻辑） |
| 850 | 252, 240 | 年化计算 | 应参数化（影响策略逻辑） |
| 915 | 42, 24 | 随机种子 | 应参数化（影响策略逻辑） |
| 917 | 1 | decision_interval默认 | 应参数化（影响策略逻辑） |
| 935 | 0.3, 3 | 开仓条件 | 应参数化（影响策略逻辑） |
| 938 | 0.02 | shadow_random概率 | 应参数化（影响策略逻辑） |
| 957 | 252, 240 | Sharpe年化 | 应参数化（影响策略逻辑） |
| 1002 | 0.03, 20, 0.5 | box参数默认 | 应参数化（影响策略逻辑） |
| 1147 | 0.20, 0.02 | spring参数默认 | 应参数化（影响策略逻辑） |
| 1268 | 5, 100.0, 0.25 | HFT参数默认 | 应参数化（影响策略逻辑） |
| 1295 | 0.005 | shadow_random概率 | 应参数化（影响策略逻辑） |
| 1302 | 0.2 | strength阈值 | 应参数化（影响策略逻辑） |
| 1412-1415 | 50.0, 30.0, 0.6, 15.0 | 套利参数默认 | 应参数化（影响策略逻辑） |
| 1517-1519 | 5.0, 5, 3 | 做市参数默认 | 应参数化（影响策略逻辑） |
| 1657 | 0.0, 0.001, 0.005... | drop_probs默认 | 应参数化（影响策略逻辑） |
| 1814 | 0.0, 0.1, 0.5... | delay_lambdas默认 | 应参数化（影响策略逻辑） |
| 1825 | 0.5 | sharpe_ratio阈值 | 应参数化（影响策略逻辑） |
| 1881 | 0.10 | 极端日阈值 | 应参数化（影响策略逻辑） |
| 1951 | 2.0 | t_stat阈值 | 应参数化（影响策略逻辑） |
| 2038 | [1.0, 5.0, 10.0, 20.0, 50.0] | slippage_multipliers | 应参数化（影响策略逻辑） |
| 2050 | 0.3 | max_dd阈值 | 应参数化（影响策略逻辑） |
| 2101 | 2.0 | t_diff阈值 | 应参数化（影响策略逻辑） |
| 2137 | 0.05 | GBM收益阈值 | 应参数化（影响策略逻辑） |
| 2118 | 0.0, 0.15 | GBM参数 | 应参数化（影响策略逻辑） |
| 2428 | 100 | min_transitions_per_regime | 应参数化（影响策略逻辑） |
| 2428 | 0.60 | min_fold_overlap | 应参数化（影响策略逻辑） |
| 2429 | 5 | n_folds | 应参数化（影响策略逻辑） |
| 2458 | 0.33, 0.66 | IV分位数 | 应参数化（影响策略逻辑） |
| 2487 | 50 | fold_size最小值 | 应参数化（影响策略逻辑） |
| 2557 | 0.3 | sharpe_spread阈值 | 应参数化（影响策略逻辑） |
| 2595 | 1.645, 1.960, 2.576 | z值表 | 统计常数，可保留 |
| 2602-2613 | 2.0, 1.0, 0.5 | CI宽度阈值 | 应参数化（影响策略逻辑） |
| 2632-2636 | L2参数网格 | L2_PARAM_GRID | 应参数化（影响策略逻辑） |
| 2740 | 10 | lookahead_bars默认 | 应参数化（影响策略逻辑） |
| 2740 | 0.55 | min_accuracy | 应参数化（影响策略逻辑） |
| 2741 | 100 | min_transitions | 应参数化（影响策略逻辑） |
| 2795 | 0.10 | tolerance | 应参数化（影响策略逻辑） |
| 2837 | 3 | min_state_transitions | 应参数化（影响策略逻辑） |
| 3007 | 1, 2, 3, 5... | MULTISCALE_BAR_LENGTHS | 应参数化（影响策略逻辑） |
| 3129 | 5 | n_ticks默认 | 应参数化（影响策略逻辑） |
| 3151 | 0.8, 0.9 | imbalance/strength衰减 | 应参数化（影响策略逻辑） |
| 3202-3207 | BAR_INTERVAL_GRID | 策略粒度映射 | 应参数化（影响策略逻辑） |
| 3209-3233 | KLINE_LENGTH_PARAM_GRID | K线参数网格 | 应参数化（影响策略逻辑） |
| 3709 | 0.5 | KL-Q1阈值 | 应参数化（影响策略逻辑） |
| 3735 | 0.5 | KL-Q3阈值 | 应参数化（影响策略逻辑） |
| 4003-4028 | 大量P0阈值 | _run_final_checks | 应参数化（影响策略逻辑） |
| 4084 | -0.30 | 样本外衰减阈值 | 应参数化（影响策略逻辑） |
| 4094 | 0.5 | 训练Sharpe阈值 | 应参数化（影响策略逻辑） |
| 4101 | 0.3 | 测试Sharpe阈值 | 应参数化（影响策略逻辑） |
| 4108 | -0.50 | 最大回撤阈值 | 应参数化（影响策略逻辑） |
| 4115 | 30 | 最少信号数 | 应参数化（影响策略逻辑） |
| 4123 | 0.8 | 逻辑反转阈值下限 | 应参数化（影响策略逻辑） |
| 4164-4167 | 0.5, 0.5, 0.3, 0.4 | Alpha阈值 | 应参数化（影响策略逻辑） |
| 4176 | 30 | Alpha占比阈值 | 应参数化（影响策略逻辑） |
| 4739-4819 | CR_PARAM_GRID | 79个参数3水平 | 应参数化（影响策略逻辑） |

### 模块排查结论
- **状态**: 最核心模块，功能最完整，但魔法数字最多
- **风险点**: 
  1. 大量硬编码参数（>100个），几乎所有策略阈值都是魔法数字
  2. 与 `advanced_validation.py` 和 `l2_optimizer.py` 存在方法名冲突
  3. `main_scheduler` 中 `PARAM_GRID_ROUND1` 和 `PARAM_GRID_ROUND2` 的组合数可能非常大，需确认内存/时间可行性
  4. `_run_final_checks` 中P0阈值全部硬编码，无法通过配置调整
  5. `run_cr_params_sweep` 中 `CR_PARAM_GRID` 有79个参数，每个3水平，总组合数巨大（3^79），随机采样可能遗漏最优解

---

## 跨模块问题汇总

### 1. 方法重复/冲突矩阵

| 方法名 | advanced_validation.py | l2_optimizer.py | task_scheduler.py | 建议 |
|--------|------------------------|-----------------|-------------------|------|
| validate_regime_robustness | DeepValidationSuite类方法(空) | - | 全局函数(完整) | 删除advanced_validation中的空实现 |
| validate_cross_strategy_correlation | DeepValidationSuite类方法(空) | - | 全局函数(完整) | 删除advanced_validation中的空实现 |
| validate_hft_temporal_robustness | DeepValidationSuite类方法(空) | - | 全局函数(完整) | 删除advanced_validation中的空实现 |
| validate_market_friendliness_baseline | DeepValidationSuite类方法(空) | - | 全局函数(完整) | 删除advanced_validation中的空实现 |
| validate_logic_transferability | DeepValidationSuite类方法(空) | - | 全局函数(完整) | 删除advanced_validation中的空实现 |
| validate_liquidity_stress | DeepValidationSuite类方法(空) | - | 全局函数(完整) | 删除advanced_validation中的空实现 |
| validate_doomed_tests | DeepValidationSuite类方法(空) | - | 全局函数(完整) | 删除advanced_validation中的空实现 |
| evaluate_state_accuracy | - | L2Optimizer类方法 | 全局函数(完整) | 统一调用task_scheduler版本 |
| check_l2_statistical_power | - | L2Optimizer类方法 | 全局函数(完整) | 统一调用task_scheduler版本 |
| check_l2_conflict | - | L2Optimizer类方法 | 全局函数(完整) | 统一调用task_scheduler版本 |
| compute_alpha_confidence_interval | - | TwelveStrategyRunner类方法 | 全局函数(完整) | 统一调用task_scheduler版本 |

### 2. 空方法/占位符汇总

| 模块 | 空方法 | 影响 |
|------|--------|------|
| advanced_validation.py | DeepValidationSuite.validate_regime_robustness | 验证永远通过 |
| advanced_validation.py | DeepValidationSuite.validate_cross_strategy_correlation | 验证永远通过 |
| advanced_validation.py | DeepValidationSuite.validate_hft_temporal_robustness | 验证永远通过 |
| advanced_validation.py | DeepValidationSuite.validate_market_friendliness_baseline | 验证永远通过 |
| advanced_validation.py | DeepValidationSuite.validate_logic_transferability | 验证永远通过 |
| advanced_validation.py | DeepValidationSuite.validate_liquidity_stress | 验证永远通过 |
| advanced_validation.py | DeepValidationSuite.validate_doomed_tests | 验证永远通过 |
| test_design_suite.py | TestDesignSuite._run_single_test | 所有测试默认通过 |

### 3. 魔法数字严重程度分级

| 严重程度 | 数量 | 说明 |
|----------|------|------|
| 高（影响策略逻辑） | ~120个 | 阈值、乘数、网格值等 |
| 中（影响性能/配置） | ~20个 | 工作进程数、缓冲区大小等 |
| 低（技术常数） | ~10个 | 数学常数、单位换算等 |

### 4. 集成完备性总结

| 模块 | 被调用数 | 调用外部数 | 状态 |
|------|----------|------------|------|
| strategy_judgment_engine.py | 3 | 5+ | 核心枢纽 |
| backtest_integration_hooks.py | 2 | 2 | 集成层 |
| strategy_behavior_diagnosis.py | 3 | 0 | 自包含 |
| advanced_validation.py | 1 | 0 | 被动态导入 |
| statistical_validation.py | 1 | 0 | 被动态导入 |
| l2_optimizer.py | 1 | 0 | 被动态导入 |
| test_design_suite.py | 1 | 0 | 被动态导入 |
| task_scheduler.py | 1 | 3+ | 核心引擎 |

---

## 整改建议优先级

### P0（立即修复）
1. **删除 `advanced_validation.py` 中 `DeepValidationSuite` 的空方法**，或将其委托到 `task_scheduler.py` 的同名函数
2. **完善 `test_design_suite.py` 中 `_run_single_test`**，实现真正的测试逻辑
3. **统一方法调用路径**，避免 `strategy_judgment_engine.py` 中同时导入类方法和全局函数的混淆

### P1（短期修复）
4. **将 `task_scheduler.py` 中的核心阈值提取为配置常量或构造参数**
5. **将 `_run_final_checks` 中的P0阈值参数化**
6. **将 `REASON_MULTIPLIERS` 参数化**

### P2（中期优化）
7. **将各策略的默认参数网格提取到配置文件**
8. **统一 `PARAM_DEFAULTS_*` 系列常量的管理**
9. **添加 `CR_PARAM_GRID` 的合理性校验（避免组合爆炸）**

### P3（长期改进）
10. **建立参数版本控制系统**
11. **添加参数敏感性分析自动化**
12. **建立跨模块方法命名规范**
