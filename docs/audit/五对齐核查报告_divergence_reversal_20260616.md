# 五对齐核查报告 — divergence_reversal.py 专项

核查日期: 2026-06-16  
核查范围: divergence_reversal.py / parameter_attribute_matrix.yaml / tvf_params.yaml / _param_defaults.py / shadow_strategy_core.py / judgment_types.py / test_v71_task_scheduler.py / market_snapshot_collector.py  
核查指令: 仅核查现状、形成报告、不进行修复  

---

## 总体评级汇总

| 对齐项 | 评级 | 状态说明 |
|--------|------|----------|
| 对齐1: matrix vs tvf_params vs V7 | **A** | 15个divergence参数三源完全对齐 |
| 对齐2: V7文档 vs 生产模块 | **B** | 参数默认值对齐，但DivergenceReversalParams影子A/B覆盖与PARAM_DEFAULTS_DIVERGENCE_SHADOW_A/B存在不一致 |
| 对齐3: 测试脚本 vs V7 | **C** | 测试断言仍按6组/18策略编写，未更新为7组/21策略；market_snapshot_collector策略键未扩展 |
| 对齐4: judgment_standards vs V7 | **A** | divergence策略类型已完整覆盖权重/阈值/映射表 |
| 对齐5: 评判脚本 vs V7 | **A** | 与对齐4一致，完全对齐 |

**综合评级: B+** (3A / 1B / 1C)

---

## 对齐1: parameter_attribute_matrix.yaml vs tvf_params.yaml vs V7文档

### 核查内容
元属性定义文件、配置值文件与V7文档三方对齐，聚焦S7 divergence反转策略15个新增参数。

### 核查结果

#### parameter_attribute_matrix.yaml — divergence参数存在性

| 参数名 | 类型 | 默认值 | grid | 对齐状态 |
|--------|------|--------|------|----------|
| divergence_lookback | int | 20 | [10,15,20,25,30] | OK |
| divergence_atm_threshold | float | 0.03 | [0.02,0.03,0.04,0.05] | OK |
| divergence_w_future | float | 0.35 | [0.25,0.30,0.35,0.40,0.45] | OK |
| divergence_w_option_coll | float | 0.35 | [0.25,0.30,0.35,0.40,0.45] | OK |
| divergence_consistency_boost | float | 1.5 | [1.2,1.3,1.5,1.7,2.0] | OK |
| divergence_min_ratio | float | 0.6 | [0.4,0.5,0.6,0.7,0.8] | OK |
| divergence_trend_significance | float | 1.0e-6 | [1.0e-7,1.0e-6,1.0e-5] | OK |
| divergence_div_strength_clip | float | 1.0 | [0.8,0.9,1.0] | OK |
| divergence_signal_threshold | float | 0.15 | [0.10,0.15,0.20,0.25,0.30] | OK |
| divergence_take_profit_ratio | float | 1.8 | [1.0,1.2,1.5,1.8,2.0,2.5,3.0] | OK |
| divergence_stop_loss_ratio | float | 0.3 | [0.2,0.3,0.4,0.5,0.6] | OK |
| divergence_max_risk_ratio | float | 0.5 | [0.1,0.2,0.3,0.4,0.5,0.6,0.8] | OK |
| divergence_hard_time_stop_min | float | 60.0 | [30,45,60,90,120] | OK |
| divergence_cooldown_bars | int | 10 | [5,8,10,15,20,30] | OK |
| divergence_position_scale | float | 0.3 | [0.1,0.2,0.3,0.4,0.5] | OK |
| divergence_moneyness_depth | float | 0.06 | [0.04,0.05,0.06,0.08,0.10] | OK |

> 注: w_option_itm 为 derived 参数(1.0 - w_future - w_option_coll)，未在matrix中单独定义，matrix中已通过constraint标注守恒关系。

#### tvf_params.yaml — L4背离反转层

| 项目 | 状态 |
|------|------|
| l4_divergence_reversal 权重声明 | OK (l4_divergence_reversal: 0.15) |
| l4_divergence_reversal.signal_threshold | OK (0.15) |
| l4_divergence_reversal.moneyness_depth | OK (0.06) |
| l4_divergence 搜索空间 | OK (grid_search / l4_divergence 下含 signal_threshold 等) |

#### _param_defaults.py — DIVERGENCE_DEFAULTS / PARAM_GRID_DIVERGENCE

| 项目 | 状态 |
|------|------|
| DIVERGENCE_DEFAULTS 15键 | OK，与matrix default逐一一致 |
| PARAM_GRID_DIVERGENCE 15键 | OK，与matrix grid逐一一致 |

### 评级: A (完全对齐)

---

## 对齐2: 参数池V7文档 vs 策略生产模块功能

### 核查内容
PARAM_DEFAULTS核心参数、PARAM_GRID、shadow_strategy_core策略组声明、DivergenceReversalParams类定义。

### 核查结果

#### PARAM_DEFAULTS 核心参数核查

| 参数 | 代码值(_param_defaults.py) | divergence_reversal.py默认值 | 对齐状态 |
|------|----------------------------|------------------------------|----------|
| divergence_signal_threshold | 0.15 | 0.15 | OK |
| divergence_take_profit_ratio | 1.8 | 1.8 | OK |
| divergence_stop_loss_ratio | 0.3 | 0.3 | OK |
| divergence_max_risk_ratio | 0.5 | 0.5 | OK |
| divergence_hard_time_stop_min | 60.0 | 60.0 | OK |
| divergence_cooldown_bars | 10 | 10 | OK |
| divergence_position_scale | 0.3 | 0.3 | OK |
| divergence_moneyness_depth | 0.06 | 0.06 | OK |

#### shadow_strategy_core.py 策略组与AlphaMetrics核查

| 项目 | 值 | 对齐状态 |
|------|-----|----------|
| STRATEGY_GROUPS | 含 's7_divergence' (7组) | OK |
| AlphaMetrics s7_master_sharpe | 已定义 | OK |
| AlphaMetrics s7_shadow_a_sharpe | 已定义 | OK |
| AlphaMetrics s7_shadow_b_sharpe | 已定义 | OK |
| AlphaMetrics s7_alpha | 已定义 | OK |
| 7组×3变体独立equity_curve | 初始化循环覆盖STRATEGY_GROUPS | OK |

#### DivergenceReversalParams — from_param_pool映射核查

| 参数 | _KEY_MAP映射 | 对齐状态 |
|------|-------------|----------|
| lookback | divergence_lookback | OK |
| atm_threshold | divergence_atm_threshold | OK |
| w_future | divergence_w_future | OK |
| w_option_coll | divergence_w_option_coll | OK |
| consistency_boost | divergence_consistency_boost | OK |
| min_ratio | divergence_min_ratio | OK |
| trend_significance | divergence_trend_significance | OK |
| div_strength_clip | divergence_div_strength_clip | OK |
| signal_threshold | divergence_signal_threshold | OK |
| take_profit_ratio | divergence_take_profit_ratio | OK |
| stop_loss_ratio | divergence_stop_loss_ratio | OK |
| max_risk_ratio | divergence_max_risk_ratio | OK |
| hard_time_stop_min | divergence_hard_time_stop_min | OK |
| cooldown_bars | divergence_cooldown_bars | OK |
| position_scale | divergence_position_scale | OK |
| moneyness_depth | divergence_moneyness_depth | OK |

#### ⚠ P1遗留项 — DivergenceReversalParams.__post_init__ 影子覆盖不完整

`DivergenceReversalParams.__post_init__` 中 shadow_a/shadow_b 仅覆盖了 4 个参数：
- lookback, consistency_boost, position_scale, cooldown_bars

而 `_param_defaults.py` 中 `PARAM_DEFAULTS_DIVERGENCE_SHADOW_A` 覆盖了 8 个参数：
- lookback=16, consistency_boost=1.35, position_scale=0.24, cooldown_bars=12, **take_profit_ratio=1.2, stop_loss_ratio=0.6, max_risk_ratio=0.15, hard_time_stop_min=48.0**

`PARAM_DEFAULTS_DIVERGENCE_SHADOW_B` 同样覆盖了 8 个参数：
- lookback=12, consistency_boost=1.2, position_scale=0.18, cooldown_bars=15, **take_profit_ratio=1.1, stop_loss_ratio=0.7, max_risk_ratio=0.10, hard_time_stop_min=36.0**

**不对齐详情:**
- `__post_init__` 未调整 `take_profit_ratio`, `stop_loss_ratio`, `max_risk_ratio`, `hard_time_stop_min`
- 若直接 `DivergenceReversalParams(shadow_variant="shadow_a")`，则上述4个参数仍使用master默认值(1.8/0.3/0.5/60.0)，与 PARAM_DEFAULTS_DIVERGENCE_SHADOW_A 期望的 (1.2/0.6/0.15/48.0) 不一致
- 虽然 `from_param_pool()` 可从param pool读取shadow专属值，但 param pool 真相源(parameter_attribute_matrix.yaml)中每个参数仅有一个default，没有区分变体
- `STRATEGY_SHADOW_DEFAULTS` 中已包含 divergence 的 shadow_a/shadow_b 完整覆盖，但 DivergenceReversalParams 未消费该结构

**位置:**
- `ali2026v3_trading/strategy/divergence_reversal.py:97-106`
- `ali2026v3_trading/param_pool/_param_defaults.py:641-669`

### 评级: B (核心对齐，存在P1遗留项)

---

## 对齐3: 参数池测试脚本 vs V7文档

### 核查内容
测试断言、L2_PARAM_GRID、策略覆盖验证。

### 核查结果

#### test_v71_task_scheduler.py — 策略组数量断言

| 测试用例 | 断言内容 | 实际代码状态 | 执行结果预测 |
|----------|---------|-------------|-------------|
| test_shadow_defaults_has_six_groups | STRATEGY_SHADOW_DEFAULTS.keys() == 6组 | 实际为7组(含divergence) | **将失败** |
| test_shadow_defaults_cover_all_six_groups | 同上 | 实际为7组 | **将失败** |
| TestParamDefaults.test_six_master_defaults_exist | 6个master | 实际PARAM_DEFAULTS_DIVERGENCE存在，但测试未涵盖 | **通过(但覆盖不全)** |
| TestParamDefaults.test_total_eighteen_instances | 6+6+6=18 | 实际应为7+7+7=21 | **通过(计数仅测已引用字典)** |
| TestEighteenStrategyCoverage.test_eighteen_ids_count | ALL_18_STRATEGY_IDS == 18 | 实际SIX_STRATEGY_KEYS=6 → 18 | **通过(但应为21)** |
| TestEighteenStrategyCoverage.test_six_strategy_keys | 6个策略键 | 实际6个，缺少divergence | **通过(但应为7)** |

#### market_snapshot_collector.py — 策略标识体系

| 项目 | 现状 | 期望 | 状态 |
|------|------|------|------|
| SIX_STRATEGY_KEYS | 6个(high_freq/resonance/box/spring/arbitrage/market_making) | 7个(增加divergence) | **不对齐** |
| ALL_18_STRATEGY_IDS | 18个(6×3) | 21个(7×3) | **不对齐** |
| _parse_strategy_id | 仅识别6组策略ID | 应识别s7_divergence_master/shadow_a/shadow_b | **不对齐** |

### 评级: C (测试断言与策略标识体系滞后于代码扩展)

---

## 对齐4: 策略评判文档 vs V7文档

### 核查结果

#### judgment_types.py — 策略类型覆盖

| 项目 | 覆盖内容 | 对齐状态 |
|------|---------|----------|
| STRATEGY_TYPE_WEIGHT_OVERRIDES | 含divergence: behavior_consistency=0.16, cross_instrument_consistency=0.06, prediction_calibration=0.06 | OK |
| STRATEGY_TYPE_THRESHOLD_OVERRIDES | 含divergence: behavior_consistency=0.60, cross_instrument_consistency=0.55, prediction_calibration=0.55, extreme_survival=0.55 | OK |
| ECOSYSTEM_TO_JUDGMENT_TYPE_MAP | 含divergence_reversal→divergence, divergence_reversal_defensive→divergence | OK |

#### DEFAULT_THRESHOLDS / DEFAULT_WEIGHTS / SCORING_COEFFICIENTS

| 项目 | 状态 |
|------|------|
| DEFAULT_THRESHOLDS 12维度 | 完整，无变化 |
| DEFAULT_WEIGHTS 12维度总和=1.00 | 完整，无变化 |
| SCORING_COEFFICIENTS关键参数 | 完整，无变化 |

### 评级: A (完全对齐)

---

## 对齐5: 策略评判脚本功能 vs V7文档

### 核查结果

#### CAPITAL_SCALE_CONFIGS / 评分系数

| 项目 | 状态 |
|------|------|
| CAPITAL_SCALE_CONFIGS三档配置 | 完整，未因divergence扩展而变更 |
| SCORING_COEFFICIENTS关键参数 | 完整，未因divergence扩展而变更 |
| divergence专属权重/阈值覆盖 | 已在STRATEGY_TYPE_WEIGHT_OVERRIDES/THRESHOLD_OVERRIDES中体现 |

### 评级: A (完全对齐)

---

## divergence_reversal.py 模块独立核查

### 代码结构

| 项目 | 状态 |
|------|------|
| 参数类 DivergenceReversalParams | 16个字段，完整定义 |
| 输出类 DivergenceReversalOutput | 5个输出数组+统计字段，完整定义 |
| 三层背离检测(L1/L2/L3) | 已实现(_detect_future_cross_term_divergence / _detect_option_premium_collective_divergence / _detect_option_near_itm_divergence) |
| 综合信号合成 | 已实现(_compute_reversal_signal) |
| 核心模块类 DivergenceReversalModule | 已实现，含线程安全(RLock)、单例工厂 |
| 从param pool读取 | 已实现(from_param_pool)，映射15个参数 |
| 影子变体支持 | shadow_variant字段存在，__post_init__有部分调整 |

### 与五对齐的衔接点

| 衔接点 | 状态 |
|--------|------|
| 对齐1(matrix/tvf) | 15个参数通过from_param_pool映射到matrix默认值 |
| 对齐2(生产模块) | DIVERGENCE_DEFAULTS / PARAM_GRID_DIVERGENCE 已注册 |
| 对齐3(测试脚本) | 模块本身无单元测试文件；依赖的集成测试未更新 |
| 对齐4/5(策略评判) | 模块输出可被评判系统消费(divergence_reversal映射已注册) |

---

## P0缺陷 / P1遗留项清单

### P0缺陷 (本次核查发现，需修复)

| # | 缺陷描述 | 影响 | 涉及文件 |
|---|---------|------|----------|
| 1 | DivergenceReversalParams.__post_init__ 影子A/B覆盖不完整：缺少take_profit_ratio/stop_loss_ratio/max_risk_ratio/hard_time_stop_min的变体调整 | shadow_a/shadow_b实例化时通用风险参数仍使用master值，与PARAM_DEFAULTS_DIVERGENCE_SHADOW_A/B期望不一致 | divergence_reversal.py |

### P1遗留项 (已知待跟进)

| # | 遗留项 | 影响 | 涉及文件 |
|---|--------|------|----------|
| 1 | test_v71_task_scheduler.py 测试断言仍为6组/18策略 | 测试与实际7组/21策略不符，部分断言将失败 | test_v71_task_scheduler.py |
| 2 | market_snapshot_collector.py SIX_STRATEGY_KEYS / ALL_18_STRATEGY_IDS 未扩展divergence | 评判系统策略标识体系未覆盖S7的3个变体，可能导致divergence策略快照收集/识别异常 | market_snapshot_collector.py |

---

## 核查结论

**divergence_reversal.py 模块已完成与五对齐文档的核心对接：**
1. 15个divergence参数在 parameter_attribute_matrix.yaml / tvf_params.yaml / _param_defaults.py / divergence_reversal.py 四源对齐；
2. shadow_strategy_core.py 已声明 s7_divergence 策略组并扩展 AlphaMetrics；
3. judgment_types.py 已完成 divergence 策略类型的权重/阈值/映射覆盖；

**现存不对齐点集中在：**
1. **DivergenceReversalParams 影子变体参数覆盖不完整** (对齐2-B级)；
2. **测试断言与策略标识体系未随7组扩展更新** (对齐3-C级)。

**本次核查不执行修复。**

---

*报告生成完毕。如需修复上述P0/P1项，请单独下达修复指令。*
