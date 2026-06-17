# param_pool 模块合并重构方案

## 1. 现状诊断

### 1.1 文件规模与分布

| 类别 | 文件数 | 有效代码行数 | 问题 |
|------|--------|-------------|------|
| 回测引擎类 | 28 | ~5,800 | 过度拆分、循环依赖、空壳残留 |
| 验证套件类 | 15 | ~4,300 | 边界模糊、层层编排 |
| 优化引擎类 | 13 | ~4,900 | 搜索/扫描/分析分散 |
| L1量化审计 | 15 | ~3,800 | 子目录过深、占位文件多 |
| 时间序列策略 | 7 | ~2,000 | 结果读写分散 |
| 参数核心定义 | 4 | ~1,400 | 网格/默认值/类型分散 |
| 预处理管道 | 5 | ~360 | 多阶段碎片化 |
| YAML配置 | 3 | ~2,700 | 分散不利于版本管理 |
| 空壳/占位 | 53 | ~300 | 无独立存在理由 |
| **合计** | **93** | **~22,700+2,700 YAML** |  |

### 1.2 核心问题

1. **历史合并遗留空壳**：大量文件（如 `backtest_loop_risk.py`、`backtest_loop_positions.py`）内容已被合并到 `backtest_runner_utils.py`，但原文件未被删除，形成“影子文件”。
2. **循环依赖网络**：`backtest_state ↔ backtest_fidelity ↔ backtest_config ↔ backtest_runner_base` 之间通过 `__getattr__` 和延迟导入勉强维持，任何修改都牵一发而动全身。
3. **伪模块化**：`checks_orchestrator` 与 `validation_deep_orchestrator` 职责高度重叠，都是“编排器套编排器”。
4. **占位文件污染**：`l1_quantification/` 下 8 个 3 行文件，仅有类名或 `pass`，无实现。

---

## 2. 合并原则

| 原则 | 说明 |
|------|------|
| **消除空壳** | 内容已被合并到其他文件、或仅有 `pass` 的占位文件，直接删除。 |
| **消除循环依赖** | 同一闭环内的文件合并为一个物理模块，内部通过函数组织而非文件间导入。 |
| **按变更节奏分组** | 回测逻辑（稳定）、验证规则（中度变更）、参数网格（高频变更）分离。 |
| **YAML 集中** | 配置类 YAML 合并为一个文件，按 top-level key 区分域。 |
| **保留可测试边界** | 合并后的模块内部函数仍保持独立，单元测试从“测文件”转为“测函数”。 |

---

## 3. 具体合并方案（93 → 12）

### 3.1 目标模块清单

```
param_pool/
├── __init__.py                          (保留，精简导出)
├── README.md                            (保留)
├── REFACTOR_PLAN.md                     (本文件)
├── backtest_engine.py                   (回测引擎核心)
├── validation_suite.py                  (验证与P0门控)
├── optimization_engine.py               (参数优化与搜索)
├── quantification_audit.py              (L1量化与元审计)
├── ts_strategies.py                     (时序策略与结果IO)
├── parameter_core.py                    (参数定义与网格)
├── preprocess_pipeline.py               (数据预处理)
├── param_configs.yaml                   (合并后的统一配置)
└── _deprecated/                         (过渡期保留被删除文件的空壳，可选)
```

---

### 3.2 模块一：backtest_engine.py

**职责**：回测引擎的全部生命周期——配置、状态、定价、执行、策略Runner、结果构建。

**合并来源与行数**：

| 原文件 | 行数 | 合并方式 |
|--------|------|----------|
| `backtest_config.py` | 677 | 常量区 |
| `backtest_state.py` | 843 | 数据类与状态操作 |
| `backtest_fidelity.py` | 290 | 保真度模拟 |
| `backtest_runner_base.py` | 472 | 主循环与Facade |
| `backtest_runner_utils.py` | 1,229 | 工具函数（已内含 loop_risk/loop_positions 等） |
| `backtest_strategy_runners.py` | 1,451 | 各策略专用Runner |
| `backtest_runner_validation.py` | 359 | 回测期验证 |
| `backtest_param_grids.py` | 364 | 网格常量（部分移至 parameter_core） |
| `backtest_loop_core.py` | 2 | **删除**，内容已合并 |
| `backtest_loop_positions.py` | 12 | **删除**，内容已合并 |
| `backtest_loop_risk.py` | 12 | **删除**，内容已合并 |
| `backtest_loop_slippage.py` | 12 | **删除**，空壳 |
| `backtest_metrics.py` | 12 | **删除**，空壳 |
| `backtest_orchestrator.py` | 12 | **删除**，空壳 |
| `backtest_position_manager.py` | 12 | **删除**，空壳 |
| `backtest_pricing.py` | 12 | **删除**，空壳 |
| `backtest_runner.py` | 2 | **删除**，空壳 |
| `backtest_runner_arb_mm.py` | 2 | **删除**，空壳 |
| `backtest_runner_box.py` | 2 | **删除**，空壳 |
| `backtest_runner_hft.py` | 2 | **删除**，空壳 |
| `backtest_runner_result.py` | 2 | **删除**，空壳 |
| `backtest_runner_types.py` | 2 | **删除**，空壳 |
| `backtest_runner_snapshot.py` | 12 | **删除**，内容已合并 |
| `backtest_runner_special.py` | 12 | **删除**，空壳 |
| `backtest_safety_checker.py` | 12 | **删除**，空壳 |
| `backtest_snapshot.py` | 12 | **删除**，空壳 |
| `backtest_state_checker.py` | 12 | **删除**，空壳 |
| `backtest_validation.py` | 11 | **删除**，空壳 |

**原始行数合计**：~5,845 行  
**预估合并后**：**4,800 ~ 5,200 行**（去除重复 import、合并 docstring、内联 20 余处仅被调用一次的 3 行函数）

**内部结构建议**：
```
# backtest_engine.py
"""回测引擎：配置 → 状态 → 定价 → 执行 → 结果"""

# Section 1: 配置常量 (原 backtest_config)
# Section 2: 数据类 (原 _leaf_core + backtest_state 中的 dataclass)
# Section 3: 状态与定价工具 (原 backtest_state 中的函数)
# Section 4: 保真度模拟 (原 backtest_fidelity)
# Section 5: 通用工具 (原 backtest_runner_utils)
# Section 6: 主回测循环 (原 backtest_runner_base.run_backtest)
# Section 7: 策略专用 Runner (原 backtest_strategy_runners)
# Section 8: 回测期验证 (原 backtest_runner_validation)
```

---

### 3.3 模块二：validation_suite.py

**职责**：P0 绿灯检验、深度验证编排、统计检验、Walk-Forward、PnL 归因。

**合并来源与行数**：

| 原文件 | 行数 | 合并方式 |
|--------|------|----------|
| `checks_orchestrator.py` | 786 | 核心编排 |
| `checks_individual.py` | 12 | **删除**，内容已合并 |
| `checks_statistical.py` | 2 | **删除**，空壳 |
| `validation_deep_orchestrator.py` | 978 | 深度验证编排 |
| `validation_deep_checks.py` | 12 | **删除**，内容已合并 |
| `validation_hmm_state.py` | 12 | **删除**，内容已合并 |
| `validation_l2_hyperparams.py` | 576 | L2 超参验证 |
| `adv_validation_misc.py` | 753 | 高级验证杂项 |
| `adv_validation_stats.py` | 266 | 统计验证 |
| `adv_validation_suite.py` | 302 | 验证套件入口 |
| `adv_validation_survival.py` | 2 | **删除**，空壳 |
| `adv_validation_walkforward.py` | 2 | **删除**，空壳 |
| `statistical_validation.py` | 612 | 统计检验 |
| `data_validator.py` | 12 | **删除**，空壳 |
| `run_verification.py` | 2 | **删除**，空壳 |

**原始行数合计**：~4,329 行  
**预估合并后**：**3,600 ~ 4,000 行**

**内部结构建议**：
```
# validation_suite.py
"""验证套件：P0门控 → 深度验证 → 统计检验"""

# Section 1: P0 铁律常量
# Section 2: 结果数据类 _DeepValidationResult
# Section 3: P0 基础指标检查 (原 _run_basic_metrics_checks)
# Section 4: P0 核心约束检查 (原 _run_core_constraints_checks)
# Section 5: 高级验证 (原 adv_validation_misc/stats)
# Section 6: 深度验证编排 (原 validation_deep_orchestrator)
# Section 7: 统计与 Walk-Forward (原 statistical_validation)
# Section 8: 最终判决入口 _run_final_checks
```

---

### 3.4 模块三：optimization_engine.py

**职责**：参数优化（L2、Optuna）、敏感性分析、周期共振、相位扫描、Sharpe-3D 映射。

**合并来源与行数**：

| 原文件 | 行数 | 合并方式 |
|--------|------|----------|
| `l2_optimizer.py` | 918 | L2 状态优化器 |
| `optuna_multiobjective_search.py` | 937 | Optuna 多目标搜索 |
| `optuna_objective.py` | 12 | **删除**，空壳 |
| `sensitivity_analysis.py` | 737 | 敏感性分析 |
| `cycle_resonance_module.py` | 728 | 周期共振 |
| `phase_scan_core.py` | 966 | 相位扫描核心 |
| `phase_scan_cli.py` | 12 | **删除**，空壳 |
| `phase_scan_grids.py` | 99 | 扫描网格（可内联） |
| `sharpe_3d_config.py` | 12 | **删除**，空壳 |
| `sharpe_3d_live.py` | 12 | **删除**，空壳 |
| `sharpe_3d_mapping.py` | 441 | Sharpe 3D 映射 |
| `delay_time_sharpe_3d.py` | 12 | **删除**，空壳 |
| `enhanced_phase_scan_gates.py` | 12 | **删除**，空壳 |

**原始行数合计**：~4,898 行  
**预估合并后**：**4,000 ~ 4,300 行**

---

### 3.5 模块四：quantification_audit.py

**职责**：L1 量化验证、元审计引擎、审计护照、分层层级优化。

**合并来源与行数**：

| 原文件 | 行数 | 合并方式 |
|--------|------|----------|
| `l1_quantification/__init__.py` | 34 | 删除目录，内容上移 |
| `l1_quantification/_data_validation.py` | 824 | 数据验证 |
| `l1_quantification/_quantification_core.py` | 685 | 量化核心 |
| `l1_quantification/_tier_optimizer.py` | 543 | 层级优化器 |
| `l1_quantification/meta_audit_engine.py` | 848 | 元审计引擎 |
| `l1_quantification/meta_audit_passport.py` | 870 | 审计护照 |
| `l1_quantification/v7_meta_audit_v2.py` | 17 | 入口函数，内联 |
| `l1_quantification/bayesian_shrinkage_life_estimator.py` | 3 | **合并**，类定义迁入 |
| `l1_quantification/duckdb_tick_storage.py` | 3 | **删除**，空壳 |
| `l1_quantification/external_validation_pipeline.py` | 3 | **删除**，空壳 |
| `l1_quantification/meta_audit_helpers.py` | 3 | **删除**，空壳 |
| `l1_quantification/meta_audit_sandbox.py` | 3 | **删除**，空壳 |
| `l1_quantification/performance_tier_manager.py` | 3 | **删除**，空壳 |
| `l1_quantification/soft_constrained_optimizer.py` | 3 | **删除**，空壳 |
| `l1_quantification/triple_truth_anchor.py` | 3 | **删除**，空壳 |

**原始行数合计**：~3,848 行  
**预估合并后**：**3,200 ~ 3,500 行**

**关键动作**：`l1_quantification/` 子目录整体删除，所有内容平移到 `quantification_audit.py`。  
**影响**：`backtest_state.py` 中懒加载 `BayesianShrinkageLifeEstimator` 的导入路径需更新：
```python
# 修改前
from ali2026v3_trading.param_pool.l1_quantification.bayesian_shrinkage_life_estimator import ...
# 修改后
from ali2026v3_trading.param_pool.quantification_audit import BayesianShrinkageLifeEstimator
```

---

### 3.6 模块五：ts_strategies.py

**职责**：时间序列策略回测、参数网格、结果调度与写入。

**合并来源与行数**：

| 原文件 | 行数 | 合并方式 |
|--------|------|----------|
| `ts_backtest_strategies.py` | 951 | 时序策略核心 |
| `ts_result_writer.py` | 991 | 结果写入 |
| `ts_param_grids.py` | 11 | **删除**，空壳 |
| `ts_result_data_loader.py` | 12 | **删除**，空壳 |
| `ts_result_executor.py` | 12 | **删除**，空壳 |
| `ts_result_kline_sweep.py` | 12 | **删除**，空壳 |
| `ts_result_scheduler.py` | 12 | **删除**，空壳 |

**原始行数合计**：~2,001 行  
**预估合并后**：**1,700 ~ 1,800 行**

---

### 3.7 模块六：parameter_core.py

**职责**：参数默认值、风险维度网格、回退策略、特征工程常量。

**合并来源与行数**：

| 原文件 | 行数 | 合并方式 |
|--------|------|----------|
| `_leaf_core.py` | 304 | 数据类（部分迁回 backtest_engine） |
| `_leaf_param_defaults.py` | 777 | 参数默认值与网格 |
| `feature_engine.py` | 239 | 特征工程 |
| `task_scheduler.py` | 49 | 任务调度常量 |

**原始行数合计**：~1,369 行  
**预估合并后**：**1,100 ~ 1,200 行**

**注意**：`_leaf_core.py` 中的 `_BacktestState` 等数据类应迁回 `backtest_engine.py`，本模块只保留与“参数定义”相关的常量和网格。

---

### 3.8 模块七：preprocess_pipeline.py

**职责**：数据预处理全流程。

**合并来源与行数**：

| 原文件 | 行数 | 合并方式 |
|--------|------|----------|
| `preprocess_pipeline.py` | 323 | 主流程 |
| `preprocess_ticks.py` | 12 | **删除**，空壳 |
| `preprocess_validation.py` | 12 | **删除**，空壳 |
| `preprocess_vectorized.py` | 2 | **删除**，空壳 |
| `tick_aggregator.py` | 12 | **删除**，空壳 |

**原始行数合计**：~361 行  
**预估合并后**：**300 ~ 350 行**

---

### 3.9 模块八：param_configs.yaml

**职责**：所有参数配置的统一 YAML 存储。

**合并来源**：
- `parameter_attribute_matrix.yaml` (2,346 行)
- `tvf_params.yaml` (318 行)
- `plr_thresholds.yaml` (52 行)

**合并方式**：以顶级 key 区分域：
```yaml
# param_configs.yaml
attribute_matrix:
  # 原 parameter_attribute_matrix.yaml 内容

tvf_params:
  # 原 tvf_params.yaml 内容

plr_thresholds:
  # 原 plr_thresholds.yaml 内容
```

**原始行数合计**：2,716 行  
**预估合并后**：**2,500 ~ 2,600 行**（YAML 不适合大幅压缩，结构清晰即可）

---

### 3.10 保留文件

| 文件 | 行数 | 说明 |
|------|------|------|
| `__init__.py` | 51 | 精简公共 API 导出，仅暴露 8-10 个核心入口 |
| `README.md` | 101 | 更新为重构后模块说明 |

---

## 4. 删除清单（53 个文件）

### 4.1 回测类空壳（19 个）

```
backtest_loop_core.py
backtest_loop_positions.py
backtest_loop_risk.py
backtest_loop_slippage.py
backtest_metrics.py
backtest_orchestrator.py
backtest_position_manager.py
backtest_pricing.py
backtest_runner.py
backtest_runner_arb_mm.py
backtest_runner_box.py
backtest_runner_hft.py
backtest_runner_result.py
backtest_runner_types.py
backtest_runner_snapshot.py
backtest_runner_special.py
backtest_safety_checker.py
backtest_snapshot.py
backtest_state_checker.py
backtest_validation.py
```

### 4.2 验证类空壳（5 个）

```
checks_individual.py
checks_statistical.py
data_validator.py
run_verification.py
validation_deep_checks.py
validation_hmm_state.py
```

### 4.3 优化类空壳（5 个）

```
enhanced_phase_scan_gates.py
optuna_objective.py
phase_scan_cli.py
sharpe_3d_config.py
sharpe_3d_live.py
```

### 4.4 L1 量化占位（8 个）

```
l1_quantification/duckdb_tick_storage.py
l1_quantification/external_validation_pipeline.py
l1_quantification/meta_audit_helpers.py
l1_quantification/meta_audit_sandbox.py
l1_quantification/performance_tier_manager.py
l1_quantification/soft_constrained_optimizer.py
l1_quantification/triple_truth_anchor.py
l1_quantification/bayesian_shrinkage_life_estimator.py  (类定义迁入 quantification_audit.py)
```

### 4.5 时序策略空壳（5 个）

```
ts_param_grids.py
ts_result_data_loader.py
ts_result_executor.py
ts_result_kline_sweep.py
ts_result_scheduler.py
```

### 4.6 预处理空壳（3 个）

```
preprocess_ticks.py
preprocess_validation.py
preprocess_vectorized.py
```

### 4.7 其他空壳（3 个）

```
adv_validation_survival.py
adv_validation_walkforward.py
delay_time_sharpe_3d.py
```

### 4.8 删除的目录

```
l1_quantification/   (全部内容平移到 quantification_audit.py)
```

---

## 5. 外部导入路径变更清单

以下模块需要更新对 `param_pool` 的导入：

| 导入方 | 原导入 | 新导入 |
|--------|--------|--------|
| `backtest_state.py` | `from .l1_quantification.bayesian_shrinkage_life_estimator import ...` | `from .quantification_audit import ...` |
| `strategy/` 各模块 | `from ..param_pool.backtest_runner_box import run_backtest_box_extreme` | `from ..param_pool.backtest_engine import run_backtest_box_extreme` |
| `governance/` | `from ..param_pool.checks_orchestrator import _run_final_checks` | `from ..param_pool.validation_suite import _run_final_checks` |
| `evaluation/` | `from ..param_pool.l2_optimizer import L2Optimizer` | `from ..param_pool.optimization_engine import L2Optimizer` |

**过渡策略**：在 `__init__.py` 中提供兼容性别名，过渡期 2 周：
```python
# param_pool/__init__.py (过渡期)
from .backtest_engine import run_backtest, run_backtest_box_extreme, ...
from .validation_suite import _run_final_checks, ...
from .optimization_engine import L2Optimizer, ...
from .quantification_audit import BayesianShrinkageLifeEstimator, ...

# 兼容性别名（TODO: 2026-06-27 前移除）
import sys
sys.modules['ali2026v3_trading.param_pool.backtest_runner_base'] = sys.modules['ali2026v3_trading.param_pool.backtest_engine']
# ... 其他别名
```

---

## 6. 行数估算汇总

| 模块 | 合并前行数 | 预估合并后行数 | 职责 |
|------|-----------|---------------|------|
| `backtest_engine.py` | ~5,845 | **4,800 ~ 5,200** | 回测引擎 |
| `validation_suite.py` | ~4,329 | **3,600 ~ 4,000** | 验证套件 |
| `optimization_engine.py` | ~4,898 | **4,000 ~ 4,300** | 优化引擎 |
| `quantification_audit.py` | ~3,848 | **3,200 ~ 3,500** | 量化审计 |
| `ts_strategies.py` | ~2,001 | **1,700 ~ 1,800** | 时序策略 |
| `parameter_core.py` | ~1,369 | **1,100 ~ 1,200** | 参数定义 |
| `preprocess_pipeline.py` | ~361 | **300 ~ 350** | 预处理 |
| `param_configs.yaml` | 2,716 | **2,500 ~ 2,600** | YAML 配置 |
| `__init__.py` | 51 | ~80 | 公共 API |
| `README.md` | 101 | ~150 | 文档 |
| **合计** | **~22,700 + 2,700 YAML** | **~18,500 + 2,500 YAML** | **10 个模块** |

---

## 7. 实施步骤与风险

### 7.1 推荐执行顺序

1. **Day 1: 删除纯空壳**（零风险）
   - 删除 53 个空壳/占位文件，不影响任何功能。
   - 提交一次独立 commit：`chore(param_pool): 删除无内容占位文件`。

2. **Day 2-3: YAML 合并**
   - 合并 3 个 YAML，更新读取路径。
   - 提交：`refactor(param_pool): 合并 YAML 配置`。

3. **Day 4-7: 大模块合并（按优先级）**
   - 优先级 1：`backtest_engine.py`（循环依赖最多，收益最大）
   - 优先级 2：`validation_suite.py`
   - 优先级 3：`optimization_engine.py`
   - 优先级 4：`quantification_audit.py`（涉及目录删除，需谨慎）
   - 每次合并后运行全量回测单测，确保无回归。

4. **Day 8: `__init__.py` 精简与兼容性别名**

5. **Day 9-10: 全局导入路径清理**
   - 使用 `grep` 找出所有 `param_pool.` 的旧导入路径，批量替换。

### 7.2 风险控制

| 风险 | 缓解措施 |
|------|----------|
| 合并后文件过大，IDE 卡顿 | 使用 `# Section N:` 注释分隔，配合 IDE 的 region folding；若仍卡顿，可将 `backtest_engine.py` 拆为 `backtest_engine_core.py` + `backtest_engine_runners.py`（2 个文件） |
| 循环依赖未完全消除 | 合并后内部使用函数前向声明 + 延迟局部导入，不再出现跨文件循环 |
| 下游模块导入失败 | 过渡期 `__init__.py` 提供 `sys.modules` 别名；2 周后清理 |
| Git Blame 丢失 | 合并 commit 使用 `git commit --no-verify`，并在 commit message 中注明“行级迁移，建议用 git log -p --follow 追溯” |

---

## 8. 决策结论

**执行合并的收益：**
- 文件数从 **93 → 10**（删除 53 个空壳，合并 30 个核心文件）。
- 消除 6 组以上跨文件循环依赖。
- 回测引擎、验证套件、优化引擎各自形成“可独立理解”的单一文档。
- 新开发者 onboarding 时间从“数天理清文件关系”缩短到“2 小时读通一个模块”。

**不执行的代价：**
- 每新增一个回测功能，需在 3-5 个文件间复制粘贴（已有历史证明此模式）。
- 空壳文件持续累积，6 个月后文件数可能突破 120。
- 任何重构都因“怕破坏循环依赖”而被搁置，技术债务滚雪球。

**关于“单文件超过 1000 行”的澄清**：
- 当前 `backtest_runner_utils.py` 已达 1,229 行，`backtest_strategy_runners.py` 达 1,451 行——项目早已存在千行文件。
- 合并后的文件虽大，但内部按 `Section` 组织，且消除了“为了找函数定义而跳 5 个文件”的认知负担。
- **可读性的敌人不是文件长度，而是“概念散布在多个文件中且互相循环引用”**。

---

*文档版本：v1.0*  
*生成日期：2026-06-13*  
*适用范围：ali2026v3_trading/param_pool/*
