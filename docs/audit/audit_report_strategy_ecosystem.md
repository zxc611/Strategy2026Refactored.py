# 策略生态与量化系统生产模块排查报告

> 生成日期: 2026-05-18
> 排查模块: 4个
> 排查维度: 方法完整性 / 集成完备性 / 方法重复冲突 / 魔法数字参数化

---

## 一、strategy_ecosystem.py 排查报告

### 1.1 模块总览

| 指标 | 数值 |
|------|------|
| 总行数 | 989 |
| 类数 | 4 (StrategySlot, CapitalRoute, EcosystemTradeRecord, StrategyEcosystem) |
| 方法数 | 约 35 个（含 dataclass 方法、静态方法、property） |

### 1.2 方法完整性检查结果

#### StrategySlot (dataclass)
| 方法 | 状态 |
|------|------|
| `to_dict()` | 完整实现 |

#### CapitalRoute (dataclass)
| 方法 | 状态 |
|------|------|
| `to_dict()` | 完整实现 |
| `total_base` (property) | 完整实现 |

#### EcosystemTradeRecord (dataclass)
| 方法 | 状态 |
|------|------|
| `to_dict()` | 完整实现 |

#### StrategyEcosystem
| 方法 | 状态 | 备注 |
|------|------|------|
| `__init__` | 完整 | 初始化逻辑完整，含治理检查器集成 |
| `set_decision_interval` | 完整 | |
| `should_make_decision` | 完整 | |
| `get_s1_params` | 完整 | |
| `get_s2_params` | 完整 | |
| `update_s1_params` | 完整 | |
| `update_s2_params` | 完整 | |
| `on_bar_update` | 完整 | |
| `_propagate_capital_scale` | 完整 | 调用 ModeEngine |
| `_propagate_capital_scale_fallback` | 完整 | fallback 到 RiskService/SignalService/StateParamManager/BoxSpringStrategy |
| `on_state_switched` | 完整 | SPM 联动回调 |
| `box_detector` (property) | 完整 | |
| `_generate_trade_id` | 完整 | |
| `route_capital` | 完整 | 含动态资金分配逻辑 |
| `switch_active_strategy` | **有 BUG** | 第 465 行使用未定义变量 `new_strategy_type`，应为 `new_state` |
| `check_mutual_exclusion` | 完整 | 含跨策略风险检查 |
| `_get_slot` | 完整 | |
| `_is_same_delta_direction` | 完整 | |
| `_check_cross_strategy_risk` | 完整 | 调用 position_service |
| `_get_position_service` | 完整 | |
| `record_spring_trade` | 完整 | |
| `process_other_strategy_signal` | 完整 | 调用 BoxDetector |
| `record_strategy_pnl` | 完整 | 含盈亏统计更新 |
| `_get_trades` | 完整 | |
| `compute_expected_value` | 完整 | |
| `compute_plr_stats` | 完整 | |
| `update_plr_stats` | 完整 | |
| `check_absolute_ev_bottomline` | 完整 | 含瀑布式评判引擎集成 |
| `get_active_strategy` | 完整 | |
| `get_capital_allocations` | 完整 | |
| `get_strategy_slots` | 完整 | |
| `get_health_status` | 完整 | 含 E12/E11 治理检查 |
| `get_stats` | 完整 | |
| `freeze_strategy_slot` | 完整 | |
| `resume_strategy` | 完整 | |

**空方法/占位符**: 未发现空方法或占位符。

**关键 BUG**: `switch_active_strategy` 第 465 行 `if new_strategy_type == 'correct_trending':` 中 `new_strategy_type` 未定义，应为 `new_state`。

### 1.3 集成完备性

**被谁调用（import + 调用）**:
- `mode_engine.py`: `get_strategy_ecosystem()` 在 `_should_make_decision`、`_get_active_params` 中被调用
- `state_param_manager.py`: `get_strategy_ecosystem()` 被调用（第 328-329 行）
- `box_spring_strategy.py`: `get_strategy_ecosystem()` 被调用（第 928-929, 1042-1043 行）
- `strategy_lifecycle_mixin.py`: `get_strategy_ecosystem()` 被调用（第 658-659 行）
- `strategy_core_service.py`: `get_strategy_ecosystem()` 被调用（第 272-273, 402-403 行）

**调用了谁**:
- `box_detector.BoxDetector` / `BoxStrategyParams` / `BoxProfile` / `ExtremeState`
- `策略评判.strategy_judgment_engine.CapitalScale`
- `mode_engine.ModeEngine` (动态导入)
- `risk_service.get_risk_service` (fallback)
- `signal_service.SignalService` / `get_signal_service` (fallback)
- `state_param_manager.StateParamManager` / `get_state_param_manager` (fallback)
- `box_spring_strategy.get_box_spring_strategy` (fallback)
- `position_service.aggregate_greeks_exposure` / `get_cross_strategy_risk_guard` / `get_position_service`
- `governance_engine` 中多个检查器 (E12, E13, MultiStateSwitch, WF6-10, E8-E10, E7, E11)
- `evaluation.cascade_judge.CascadeJudge` / `adapt_backtest_result`

### 1.4 方法重复/冲突

| 方法名 | 其他模块 | 冲突说明 |
|--------|----------|----------|
| `compute_expected_value` | `shadow_strategy_engine.py` | 两者都计算期望值，但 `shadow` 版本是静态方法且仅用于影子策略，`ecosystem` 版本用于主策略生态，逻辑不冲突 |
| `get_health_status` | `shadow_strategy_engine.py` / `mode_engine.py` | 各模块独立实现，返回结构不同，无直接冲突 |
| `get_stats` | `shadow_strategy_engine.py` | 两者都返回统计信息，但数据结构不同，无冲突 |
| `route_capital` | 无 | 独有方法 |
| `check_mutual_exclusion` | 无 | 独有方法 |

### 1.5 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 62 | `target_plr: float = 2.0` | StrategySlot 默认目标盈亏比 | **建议参数化**：影响策略评判逻辑 |
| 83 | `master_base: float = 0.60` | CapitalRoute 主策略基础占比 | **建议参数化**：资金分配核心参数 |
| 84 | `reverse_base: float = 0.25` | CapitalRoute 反向策略基础占比 | **建议参数化** |
| 85 | `other_base: float = 0.15` | CapitalRoute 震荡策略基础占比 | **建议参数化** |
| 87 | `master_active_boost: float = 0.15` | 激活状态资金提升 | **建议参数化** |
| 88 | `min_maintenance_ratio: float = 0.05` | 最低维持比例 | **建议参数化** |
| 130 | `MIN_TRADES_FOR_EV = 5` | 计算期望值最小交易数 | **建议参数化**：影响 EV 底线判断 |
| 178-181 | `maxlen=10000` | 交易记录队列长度 | 可保留（容量常量） |
| 191 | `_ev_cache_ttl: float = 5.0` | EV 缓存 TTL | 可保留（性能优化常量） |
| 194-198 | `1, 1, 5, 5` | 决策间隔分钟数 | **建议参数化**：影响策略响应频率 |
| 202 | `signal_confirm_ticks: 3` | S1 参数 | **建议参数化** |
| 203 | `cooldown_ms: 50` | S1 参数 | **建议参数化** |
| 204 | `min_imbalance: 0.15` | S1 参数 | **建议参数化** |
| 205 | `hard_stop_minutes: 30` | S1 参数 | **建议参数化** |
| 206 | `daily_drawdown_pct: 0.03` | S1 参数 | **建议参数化** |
| 207 | `risk_ratio: 0.2` | S1 参数 | **建议参数化** |
| 212 | `state_confirm_bars: 2` | S2 参数 | **建议参数化** |
| 213 | `cooldown_sec: 0` | S2 参数 | **建议参数化** |
| 214 | `min_strength: 0.3` | S2 参数 | **建议参数化** |
| 215 | `hard_stop_minutes: 90` | S2 参数 | **建议参数化** |
| 216 | `daily_drawdown_pct: 0.05` | S2 参数 | **建议参数化** |
| 217 | `risk_ratio: 0.3` | S2 参数 | **建议参数化** |
| 317 | `min_estimated_plr=2.0` | small 规模 PLR 过滤 | **建议参数化** |
| 319 | `min_estimated_plr=1.5` | medium 规模 PLR 过滤 | **建议参数化** |
| 375-378 | `0.35, 0.30, 0.20, 0.15` | SMALL 规模资金分配 | **建议参数化** |
| 380-383 | `0.05, 0.50, 0.30, 0.15` | LARGE 规模资金分配 | **建议参数化** |
| 399 | `remaining * 0.5, 0.3, 0.2` | 剩余资金分配比例 | **建议参数化** |
| 406 | `remaining * 0.5, 0.3, 0.2` | 剩余资金分配比例 | **建议参数化** |
| 415 | `remaining * 0.4, 0.4, 0.2` | 剩余资金分配比例 | **建议参数化** |
| 425 | `abs(total - 1.0) > 1e-6` | 浮点精度阈值 | 可保留（技术常量） |
| 517 | `other_slot.win_loss_ratio >= other_slot.target_plr` | spring 互斥通过条件 | **建议参数化**：影响互斥规则 |

### 1.6 该模块排查结论

- **整体状态**: 功能较完整，但存在一个 **运行时 BUG**（`switch_active_strategy` 中未定义变量 `new_strategy_type`）。
- **集成状态**: 高度集成，被 5 个生产模块调用，主动调用 10+ 个外部模块。
- **魔法数字**: S1/S2 参数字典、资金分配比例、PLR 阈值等核心策略参数均为硬编码，强烈建议提取到配置或 `ModeConfig` 中。
- **建议**: 修复第 465 行变量名错误；将 `_s1_params`、`_s2_params`、资金分配比例等硬编码参数化。

---

## 二、ProductionQuantSystem.py 排查报告

### 2.1 模块总览

| 指标 | 数值 |
|------|------|
| 总行数 | 331 |
| 类数 | 1 (ProductionQuantSystem) |
| 方法数 | 约 9 个 |

### 2.2 方法完整性检查结果

| 方法 | 状态 | 备注 |
|------|------|------|
| `__init__` | 完整 | 初始化 6 个量化核心模块 + 基础设施 |
| `initialize` | 完整 | 注册单例、启动热配置监听、Numba warmup |
| `update_tick` | 完整 | 核心 tick 更新循环，含 CRM 联动 |
| `periodic_scan` | 完整 | 协整扫描 |
| `persist_state` | 完整 | 状态持久化 |
| `restore_state` | 完整 | 状态恢复 |
| `get_system_status` | 完整 | 系统状态报告 |
| `shutdown` | 完整 | 优雅关闭 |

**空方法/占位符**: 未发现。

### 2.3 集成完备性

**被谁调用**:
- 仅在本模块内通过 `get_production_quant_system()` 使用，**无其他生产模块直接 import 或调用**。
- 搜索结果显示只有 `ProductionQuantSystem.py` 自身和备份目录中出现该名称。

**调用了谁**:
- `quant_infra.NumpyRingBuffer`
- `quant_platform.ExchangeTime` / `TickAggregator` / `AtomicSystemState` / `SystemHealthMonitor`
- `quant_core.MultiPeriodTrendScorer` / `IVSurfacePCA` / `AdaptiveHMM` / `VolatilityRegimeFilter` / `CointegrationScanner` / `SurvivalAnalyzer`
- `quant_services.LightweightPersistence` / `HotConfigManager` / `SingletonManager` / `numba_helper`
- `health_check_api.HealthCheckAPI` / `StructuredJsonlLogger` (动态导入)
- `参数池.cycle_resonance_module.get_cycle_resonance_module` (动态导入)

### 2.4 方法重复/冲突

| 方法名 | 其他模块 | 冲突说明 |
|--------|----------|----------|
| `get_system_status` | `mode_engine.py` / `shadow_strategy_engine.py` / `strategy_ecosystem.py` | 各模块独立实现，无冲突 |
| `shutdown` | 多个模块 | 门面类特有的关闭逻辑，无冲突 |

### 2.5 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 49 | `(5, 20, 60)` | 趋势周期默认值 | **建议参数化**：已通过 `cfg.get` 暴露，但默认值硬编码 |
| 50 | `(0.2, 0.5, 0.3)` | 趋势权重默认值 | **建议参数化** |
| 51 | `adx_period=14` | ADX 周期 | **建议参数化** |
| 55 | `pca_window=120` | PCA 窗口 | **建议参数化** |
| 56 | `pca_shrinkage=0.5` | PCA 收缩 | **建议参数化** |
| 57 | `pca_var_threshold=0.90` | PCA 方差阈值 | **建议参数化** |
| 58 | `pca_max_components=3` | PCA 最大组件数 | **建议参数化** |
| 59 | `pca_refit_interval=10` | PCA 重拟合间隔 | **建议参数化** |
| 60 | `pca_ewma_alpha=0.05` | PCA EWMA 平滑 | **建议参数化** |
| 64 | `hmm_n_states=3` | HMM 状态数 | **建议参数化** |
| 65 | `hmm_update_interval=100` | HMM 更新间隔 | **建议参数化** |
| 66 | `hmm_var_floor_ratio=0.01` | HMM 方差下限比例 | **建议参数化** |
| 67 | `hmm_min_variance=1e-10` | HMM 最小方差 | 可保留（数值稳定性常量） |
| 71 | `vol_lookback=100` | 波动率回望 | **建议参数化** |
| 72 | `vol_low_pct=25.0` | 低波动率分位 | **建议参数化** |
| 73 | `vol_high_pct=75.0` | 高波动率分位 | **建议参数化** |
| 74 | `vol_min_hold=20` | 波动率最小持有 tick | **建议参数化** |
| 75 | `vol_ewma_alpha=0.05` | 波动率 EWMA | **建议参数化** |
| 79 | `coint_window=120` | 协整窗口 | **建议参数化** |
| 80 | `coint_scan_interval=50` | 协整扫描间隔 | **建议参数化** |
| 84 | `survival_max_iter=10` | 生存分析最大迭代 | **建议参数化** |
| 85 | `survival_tol=1e-3` | 生存分析收敛容差 | 可保留（数值常量） |
| 86 | `survival_lm_damping=1e-4` | LM 阻尼 | 可保留（数值常量） |
| 91 | `exchange='DCE'` | 默认交易所 | **建议参数化** |
| 92 | `night_end_hour=23` | 夜盘结束小时 | **建议参数化** |
| 95 | `bar_interval_sec=300` | K线间隔秒 | **建议参数化** |
| 96 | `bar_vol_threshold=0` | 成交量阈值 | **建议参数化** |
| 108 | `snapshot_interval_ms=5000` | 快照间隔 | **建议参数化** |
| 113 | `config_poll_interval=5.0` | 热配置轮询间隔 | **建议参数化** |
| 121 | `imbalance_window=20` | 失衡窗口 | **建议参数化** |
| 156 | `max(cum_volume - ..., 1)` | 最小成交量 1 | 可保留（防除零） |
| 163 | `>= 3` | 失衡计算最小样本 | 可保留（技术阈值） |
| 175 | `[0.33, 0.34, 0.33]` | HMM 后验默认值 | 可保留（均匀分布回退） |
| 300 | `join(timeout=2.0)` | EM 线程等待超时 | 可保留（系统常量） |

### 2.6 该模块排查结论

- **整体状态**: 作为门面类，实现完整，所有核心方法均有实现。
- **集成状态**: **孤立模块**——无其他生产代码调用它。它只作为量化基础设施的聚合门面存在，但未被上层策略代码使用。需要确认是否被主入口（如 `main.py` 或 `strategy_core_service.py`）通过动态导入使用。
- **魔法数字**: 大量量化模块默认参数硬编码，虽然通过 `cfg.get(..., default)` 暴露，但默认值本身应集中管理。
- **建议**: 确认该模块是否被实际使用；若使用，建议将默认值集中到配置文件；若未使用，考虑移除或接入主流程。

---

## 三、mode_engine.py 排查报告

### 3.1 模块总览

| 指标 | 数值 |
|------|------|
| 总行数 | 894 |
| 类数 | 7 (CapitalMode, TakeProfitMethod, StopLossMethod, DrawdownAction, PyramidingRule, ModeConfig, ExitRuleEngine, DrawdownManager, SixDimPositionAdjustmentFactor, DefensiveDrawdownChecker, ModeEngine) |
| 方法数 | 约 28 个（含函数、staticmethod、classmethod、property） |

### 3.2 方法完整性检查结果

#### 枚举类
全部完整，无方法。

#### ModeConfig (frozen dataclass)
完整，纯数据容器。

#### ExitRuleEngine
| 方法 | 状态 | 备注 |
|------|------|------|
| `__init__` | 完整 | |
| `take_profit_method` (property) | 完整 | |
| `stop_loss_method` (property) | 完整 | |
| `compute_take_profit_levels` | 完整 | 含 TIERED/TRAILING/FIXED 三种逻辑 |
| `compute_stop_loss` | 完整 | 含 VOLATILITY/TIME_DECAY/FIXED 三种逻辑 |

#### DrawdownManager
| 方法 | 状态 | 备注 |
|------|------|------|
| `__init__` | 完整 | |
| `action` (property) | 完整 | |
| `recovery_target` (property) | 完整 | |
| `update_drawdown` | 完整 | |
| `should_reduce_size` | 完整 | |
| `should_halt_new` | 完整 | |
| `should_full_stop` | 完整 | 硬编码 `-0.05` 阈值 |
| `is_recovered` | 完整 | |

#### SixDimPositionAdjustmentFactor
| 方法 | 状态 | 备注 |
|------|------|------|
| `compute_adjustment` | 完整 | 六维因子计算 |
| `_sigmoid_adjust` (static) | 完整 | |

#### DefensiveDrawdownChecker
| 方法 | 状态 | 备注 |
|------|------|------|
| `check` | 完整 | 减仓因子计算 |

#### 模块级函数
| 方法 | 状态 | 备注 |
|------|------|------|
| `kelly_fraction` | 完整 | |
| `kelly_position_size` | 完整 | |

#### ModeEngine
| 方法 | 状态 | 备注 |
|------|------|------|
| `get_instance` (classmethod) | 完整 | 单例模式 |
| `reset_instance` (classmethod) | 完整 | |
| `create_engine` (classmethod) | 完整 | 工厂方法 |
| `__init__` | 完整 | |
| `config` (property) | 完整 | |
| `scale_str` (property) | 完整 | |
| `exit_engine` (property) | 完整 | |
| `drawdown_manager` (property) | 完整 | |
| `register_component` | 完整 | |
| `switch_mode` | 完整 | 含原子性切换 + 回滚 |
| `filter_signal_by_mode` | 完整 | 信号过滤 |
| `compute_position_size` | 完整 | 仓位计算（集成六维因子） |
| `_apply_to_risk_service` | 完整 | 模式传播 |
| `_apply_to_signal_service` | 完整 | 模式传播 |
| `_apply_to_state_param_manager` | 完整 | 模式传播 |
| `_apply_to_box_spring_strategy` | 完整 | 模式传播 |
| `_rollback` | 完整 | 回滚机制 |
| `evaluate_strategy_fit` | 完整 | 集成 CascadeJudge |
| `_should_make_decision` | 完整 | 调用 strategy_ecosystem |
| `_get_active_params` | 完整 | 调用 strategy_ecosystem |
| `auto_select_mode` | 完整 | 自动模式选择 |

**空方法/占位符**: 未发现。

### 3.3 集成完备性

**被谁调用**:
- `strategy_ecosystem.py`: `ModeEngine.create_engine()` 在 `_propagate_capital_scale` 中被调用
- `signal_service.py`: `ModeEngine.get_instance()` 在信号过滤中被调用（第 174-175 行）
- `risk_service.py`: `ModeEngine.get_instance()` 在 `compute_position_size` 中被调用（第 1008-1009 行）
- `strategy_judgment_engine.py`: 被引用
- `tvf_param_loader.py`: 被引用
- `参数池/optuna_multiobjective_search.py` / `enhanced_phase_scan.py` / `task_scheduler.py`: 被引用

**调用了谁**:
- `risk_service.get_risk_service`
- `signal_service.get_signal_service`
- `state_param_manager.get_state_param_manager`
- `box_spring_strategy.get_box_spring_strategy`
- `strategy_ecosystem.get_strategy_ecosystem`
- `evaluation.cascade_judge.CascadeJudge` / `adapt_backtest_result`

### 3.4 方法重复/冲突

| 方法名 | 其他模块 | 冲突说明 |
|--------|----------|----------|
| `compute_position_size` | `risk_service.py` | `risk_service` 调用 `ModeEngine.compute_position_size`，无冲突，是调用关系 |
| `filter_signal_by_mode` | `signal_service.py` | `signal_service` 调用此方法，无冲突 |
| `kelly_fraction` / `kelly_position_size` | 无其他模块 | 独有函数 |
| `switch_mode` | 无 | 独有方法 |

### 3.5 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 68 | `win_loss_ratio_full_score_at: float = 2.0` | ModeConfig 字段 | **已参数化**（dataclass 字段） |
| 69 | `profit_factor_full_score_at: float = 1.5` | ModeConfig 字段 | **已参数化** |
| 70 | `sharpe_full_score_at: float = 2.0` | ModeConfig 字段 | **已参数化** |
| 71 | `calmar_full_score_at: float = 2.0` | ModeConfig 字段 | **已参数化** |
| 72 | `recovery_efficiency_full_score_at: float = 2.0` | ModeConfig 字段 | **已参数化** |
| 73 | `max_consecutive_losses_full: int = 3` | ModeConfig 字段 | **已参数化** |
| 74 | `max_consecutive_losses_zero: int = 10` | ModeConfig 字段 | **已参数化** |
| 75 | `drawdown_recovery_max_hours: float = 48.0` | ModeConfig 字段 | **已参数化** |
| 76 | `extreme_max_recovery_hours: float = 72.0` | ModeConfig 字段 | **已参数化** |
| 77 | `overall_pass_threshold: float = 0.65` | ModeConfig 字段 | **已参数化** |
| 78 | `overall_conditional_threshold: float = 0.50` | ModeConfig 字段 | **已参数化** |
| 82 | `consecutive_loss_limit: int = 7` | ModeConfig 字段 | **已参数化** |
| 83 | `recovery_timeout_seconds: float = 3600.0` | ModeConfig 字段 | **已参数化** |
| 84 | `time_decay_min_days: int = 5` | ModeConfig 字段 | **已参数化** |
| 85 | `risk_pct_default: float = 0.02` | ModeConfig 字段 | **已参数化** |
| 87 | `profit_multiplier: float = 1.5` | ModeConfig 字段 | **已参数化** |
| 88 | `loss_multiplier: float = 0.7` | ModeConfig 字段 | **已参数化** |
| 89-116 | 大量 TVF 阈值 | ModeConfig 字段 | **已参数化** |
| 121 | `max_positions=12` | SHARPE_CONFIG | **已参数化**（配置实例） |
| 122 | `single_position_cap=0.08` | SHARPE_CONFIG | **已参数化** |
| 129 | `min_signal_strength=0.75` | SHARPE_CONFIG | **已参数化** |
| 130 | `time_decay_cutoff=0.03` | SHARPE_CONFIG | **已参数化** |
| 148 | `kelly_fraction=0.33` | SHARPE_CONFIG | **已参数化** |
| 188 | `max_positions=3` | PROFIT_CONFIG | **已参数化** |
| 189 | `single_position_cap=0.40` | PROFIT_CONFIG | **已参数化** |
| 196 | `min_signal_strength=0.55` | PROFIT_CONFIG | **已参数化** |
| 197 | `time_decay_cutoff=0.08` | PROFIT_CONFIG | **已参数化** |
| 212 | `kelly_fraction=0.0` | PROFIT_CONFIG | **已参数化** |
| 252 | `max_positions=6` | BALANCED_CONFIG | **已参数化** |
| 253 | `single_position_cap=0.15` | BALANCED_CONFIG | **已参数化** |
| 260 | `min_signal_strength=0.65` | BALANCED_CONFIG | **已参数化** |
| 261 | `time_decay_cutoff=0.05` | BALANCED_CONFIG | **已参数化** |
| 279 | `kelly_fraction=0.0` | BALANCED_CONFIG | **已参数化** |
| 317-321 | `'small'/'medium'/'large'` | CAPITAL_MODE_CONFIGS 键 | 可保留（规模标识符） |
| 342 | `0.50, volatility_1d, 0.50` | TIERED 止盈第一层 | **建议参数化**：影响止盈逻辑 |
| 343 | `0.30, 2*volatility_1d, 0.30` | TIERED 止盈第二层 | **建议参数化** |
| 344 | `0.20, None, 0.20` | TIERED 止盈第三层 | **建议参数化** |
| 348 | `entry_price * 1.05, 0.10` | TRAILING 止盈 | **建议参数化** |
| 352 | `entry_price * 1.10, 1.0` | FIXED 止盈 | **建议参数化** |
| 359 | `volatility_20d * 1.5` | VOLATILITY 止损乘数 | **建议参数化** |
| 396 | `< -0.05` | FULL_STOP 阈值 | **建议参数化**：影响风控 |
| 450 | `0.5 + cvd_divergence * 0.5` | CVD 因子计算 | 可保留（公式内常数） |
| 460 | `1.0 - abs(delta_exposure)` | Delta 因子 | 可保留（公式内常数） |
| 519 | `win_rate <= 0 or win_rate >= 1` | Kelly 边界检查 | 可保留（数学约束） |
| 528 | `max_cap: float = 0.08` | kelly_position_size 默认上限 | **建议参数化** |
| 537 | `risk_per_share < 1e-10` | 除零保护 | 可保留（技术常量） |
| 886 | `sharpe >= 2.0 and sortino >= 1.5` | auto_select_mode large 阈值 | **建议参数化**：影响模式选择 |
| 888 | `plr >= 2.0` | auto_select_mode small 阈值 | **建议参数化** |

### 3.6 该模块排查结论

- **整体状态**: 实现非常完整，设计良好，含原子性切换、回滚机制、六维仓位调整等高级功能。
- **集成状态**: 被 4 个生产模块调用（strategy_ecosystem, signal_service, risk_service, 以及参数池模块），并主动调用 6 个外部模块，集成完备。
- **魔法数字**: 绝大多数参数已通过 `ModeConfig` dataclass 参数化，仅 `ExitRuleEngine` 内的止盈止损计算硬编码了比例和乘数，建议提取到 `ModeConfig`。
- **建议**: 将 `ExitRuleEngine.compute_take_profit_levels` 和 `compute_stop_loss` 中的硬编码比例（如 0.50/0.30/0.20、1.05、1.10、1.5）提取到 `ModeConfig`；将 `auto_select_mode` 中的阈值参数化。

---

## 四、shadow_strategy_engine.py 排查报告

### 4.1 模块总览

| 指标 | 数值 |
|------|------|
| 总行数 | 907 |
| 类数 | 4 (ShadowTradeRecord, AlphaMetrics, ShadowParamsSnapshot, ShadowStrategyEngine) |
| 方法数 | 约 26 个 |

### 4.2 方法完整性检查结果

#### ShadowTradeRecord (dataclass)
| 方法 | 状态 |
|------|------|
| `to_dict()` | 完整 |

#### AlphaMetrics (dataclass)
| 方法 | 状态 |
|------|------|
| `to_dict()` | 完整 |

#### ShadowParamsSnapshot (dataclass)
| 方法 | 状态 |
|------|------|
| `to_dict()` | 完整 |

#### ShadowStrategyEngine
| 方法 | 状态 | 备注 |
|------|------|------|
| `__init__` | 完整 | 含参数锁定、日志队列初始化 |
| `_generate_trade_id` | 完整 | |
| `_compute_param_hash` | 完整 | MD5 哈希 |
| `_load_and_lock_params` | 完整 | 参数锁定逻辑 |
| `_load_yaml_param_sets` | 完整 | YAML 加载 |
| `_default_param_sets` (static) | 完整 | 默认参数集 |
| `are_params_locked` | 完整 | |
| `get_shadow_a_params` | 完整 | |
| `get_shadow_b_params` | 完整 | |
| `get_params_snapshot` | 完整 | |
| `relock_params` | 完整 | 谨慎使用 |
| `process_shadow_a_signal` | 完整 | 反向逻辑 |
| `process_shadow_b_signal` | 完整 | 随机逻辑 |
| `record_master_trade` | 完整 | 主策略记录 |
| `record_shadow_pnl` | 完整 | 影子盈亏记录 |
| `update_equity_curves` | 完整 | |
| `_compute_sharpe` (static) | 完整 | |
| `_compute_max_drawdown` (static) | 完整 | |
| `_equity_to_returns` (static) | 完整 | |
| `_compute_expected_value` (static) | 完整 | |
| `compute_alpha_metrics` | 完整 | Alpha 比率计算 |
| `is_degradation_active` | 完整 | |
| `is_absolute_ev_paused` | 完整 | |
| `clear_degradation` | 完整 | |
| `clear_absolute_ev_pause` | 完整 | |
| `get_alpha_ratio` | 完整 | |
| `get_master_expected_value` | 完整 | |
| `_enqueue_trade_log` | 完整 | |
| `_async_flush_log_queue` | 完整 | |
| `_do_write_logs` (static) | 完整 | |
| `generate_daily_summary` | 完整 | |
| `generate_weekly_summary` | 完整 | |
| `process_signal` | 完整 | 同时处理 A+B |
| `process_signals_batch` | 完整 | 批量处理（优化锁粒度） |
| `get_health_status` | 完整 | |
| `get_stats` | 完整 | |
| `get_recent_trades` | 完整 | |

**空方法/占位符**: 未发现。

### 4.3 集成完备性

**被谁调用**:
- `strategy_core_service.py`: `get_shadow_strategy_engine()` 被调用（第 299-300, 391-392 行）
- `tests/test_shadow_strategy_engine.py`: 测试文件调用

**调用了谁**:
- 无直接 import 其他生产模块，纯自包含模块。
- 使用标准库：`yaml`（可选依赖）、`json`、`hashlib`、`random`、`threading`、`datetime` 等。

### 4.4 方法重复/冲突

| 方法名 | 其他模块 | 冲突说明 |
|--------|----------|----------|
| `_compute_expected_value` | `strategy_ecosystem.py` | 两者都计算期望值，`shadow` 版本是静态方法，逻辑一致但用途不同，无冲突 |
| `_compute_sharpe` | 无 | 独有方法 |
| `_compute_max_drawdown` | 无 | 独有方法 |
| `get_health_status` | 多个模块 | 各模块独立实现，无冲突 |
| `get_stats` | `strategy_ecosystem.py` | 两者都返回统计，数据结构不同，无冲突 |

### 4.5 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 111 | `ALPHA_DECLINE_THRESHOLD_PCT = 20.0` | Alpha 衰减阈值 | **建议参数化**：影响降级触发 |
| 112 | `CONSECUTIVE_DECLINE_LIMIT = 2` | 连续衰减限制 | **建议参数化** |
| 113 | `MIN_TRADES_FOR_METRICS = 5` | 最小交易数 | **建议参数化** |
| 114 | `EQUITY_CURVE_MAX_LEN = 10000` | 权益曲线最大长度 | 可保留（容量常量） |
| 115 | `TRADES_MAX_LEN = 10000` | 交易记录最大长度 | 可保留（容量常量） |
| 116 | `ALPHA_HISTORY_MAX_LEN = 1000` | Alpha 历史最大长度 | 可保留（容量常量） |
| 117 | `LOG_QUEUE_MAX_LEN = 50000` | 日志队列最大长度 | 可保留（容量常量） |
| 118 | `LOG_FLUSH_INTERVAL = 100` | 日志刷盘间隔 | 可保留（性能常量） |
| 247 | `option_width_min_threshold: 2.0` | 默认参数 correct_trending | **建议参数化**：默认参数集硬编码 |
| 248 | `signal_cooldown_sec: 15` | 默认参数 correct_trending | **建议参数化** |
| 249 | `close_take_profit_ratio: 2.5` | 默认参数 correct_trending | **建议参数化** |
| 250 | `close_stop_loss_ratio: 0.4` | 默认参数 correct_trending | **建议参数化** |
| 251 | `max_risk_ratio: 0.8` | 默认参数 correct_trending | **建议参数化** |
| 252 | `max_signals_per_window: 10` | 默认参数 correct_trending | **建议参数化** |
| 253 | `lots_min: 5` | 默认参数 correct_trending | **建议参数化** |
| 256-272 | 大量数字 | incorrect_reversal / other 默认参数 | **建议参数化** |
| 320-323 | `reverse_map` | 方向反转映射 | 可保留（逻辑常量） |
| 355 | `random.choice(['long', 'short'])` | 随机选择 | 可保留（影子B设计） |
| 455 | `annualize_factor: float = 252.0` | 年化因子 | **建议参数化**：交易日数 |
| 488 | `abs(prev) < 1e-10` | 除零保护 | 可保留（技术常量） |
| 514 | `master_max_dd < 1e-10` | 除零保护 | 可保留（技术常量） |
| 515 | `master_max_dd = 1.0` | 默认最大回撤 | 可保留（回退值） |
| 530 | `* 100.0` | 百分比转换 | 可保留（单位转换） |
| 533 | `> self.ALPHA_DECLINE_THRESHOLD_PCT` | 衰减判断 | 已参数化（类常量） |
| 539 | `>= self.MIN_TRADES_FOR_METRICS` | 交易数判断 | 已参数化（类常量） |
| 615 | `datetime.now().strftime('%Y%m%d')` | 日志文件名日期格式 | 可保留（格式常量） |
| 678 | `t.timestamp.startswith(today_str)` | 日汇总筛选 | 可保留（日期匹配逻辑） |
| 713 | `now.weekday()` | 周汇总计算 | 可保留（日历逻辑） |
| 781 | `> self._summary_interval_hours * 3600` | 汇总触发间隔 | 已参数化（构造函数参数） |
| 838 | 同上 | process_signals_batch 中 | 同上 |

### 4.6 该模块排查结论

- **整体状态**: 实现完整，设计良好，含参数独立锁定、Alpha 衰减监控、批量处理优化等。
- **集成状态**: 被 `strategy_core_service.py` 调用，但调用点较少（2 处）。作为影子策略监控引擎，其独立性是设计意图。
- **魔法数字**: 类常量（ALPHA_DECLINE_THRESHOLD_PCT 等）已以类变量形式存在，但默认参数集 `_default_param_sets` 中大量硬编码参数应提取到 YAML 或配置中。
- **建议**: 将 `_default_param_sets` 中的参数提取到外部配置；将 `annualize_factor=252.0` 参数化（不同市场交易日数不同）。

---

## 五、跨模块综合对比

### 5.1 方法重复矩阵

| 方法名 | strategy_ecosystem | ProductionQuantSystem | mode_engine | shadow_strategy_engine |
|--------|:------------------:|:---------------------:|:-----------:|:----------------------:|
| `compute_expected_value` | 是 | 否 | 否 | 是（static） |
| `get_health_status` | 是 | 否（get_system_status） | 否 | 是 |
| `get_stats` | 是 | 否 | 否 | 是 |
| `get_system_status` | 否 | 是 | 否 | 否 |
| `switch_mode` | 否 | 否 | 是 | 否 |
| `compute_position_size` | 否 | 否 | 是 | 否 |
| `shutdown` | 否 | 是 | 否 | 否 |

**结论**: 无直接方法名冲突，各模块职责分离清晰。

### 5.2 集成关系图

```
strategy_ecosystem.py
    ├── 调用 mode_engine.ModeEngine
    ├── 调用 risk_service (fallback)
    ├── 调用 signal_service (fallback)
    ├── 调用 state_param_manager (fallback)
    ├── 调用 box_spring_strategy (fallback)
    ├── 调用 position_service
    ├── 调用 governance_engine
    └── 被 state_param_manager / box_spring_strategy / strategy_lifecycle_mixin / strategy_core_service 调用

ProductionQuantSystem.py
    ├── 调用 quant_infra / quant_platform / quant_core / quant_services
    ├── 调用 health_check_api (动态)
    ├── 调用 cycle_resonance_module (动态)
    └── 未被其他生产模块直接调用（孤立模块）

mode_engine.py
    ├── 调用 risk_service / signal_service / state_param_manager / box_spring_strategy
    ├── 调用 strategy_ecosystem
    ├── 调用 cascade_judge
    └── 被 strategy_ecosystem / signal_service / risk_service / 参数池模块 调用

shadow_strategy_engine.py
    ├── 无外部生产模块依赖
    └── 被 strategy_core_service 调用
```

### 5.3 关键问题汇总

| 优先级 | 模块 | 问题 | 类型 |
|--------|------|------|------|
| **P0-紧急** | strategy_ecosystem.py:465 | `switch_active_strategy` 使用未定义变量 `new_strategy_type` | 运行时 BUG |
| P1-高 | strategy_ecosystem.py | S1/S2 参数字典、资金分配比例大量硬编码 | 魔法数字 |
| P1-高 | mode_engine.py | ExitRuleEngine 止盈止损计算硬编码比例 | 魔法数字 |
| P2-中 | ProductionQuantSystem.py | 未被任何生产模块调用，存在性需确认 | 集成完备性 |
| P2-中 | shadow_strategy_engine.py | `_default_param_sets` 硬编码 | 魔法数字 |
| P3-低 | 多个模块 | `get_health_status` / `get_stats` 等重复命名 | 命名重复（无逻辑冲突） |

---

## 六、总体结论与建议

1. **立即修复**: `strategy_ecosystem.py` 第 465 行 `new_strategy_type` 应改为 `new_state`，否则 `switch_active_strategy` 在运行时必抛 `NameError`。

2. **参数化建议**: 
   - `strategy_ecosystem.py` 中的 `_s1_params`、`_s2_params`、资金分配比例应提取到配置。
   - `mode_engine.py` 中 `ExitRuleEngine` 的止盈止损比例应纳入 `ModeConfig`。
   - `shadow_strategy_engine.py` 的 `_default_param_sets` 应完全外部化到 YAML。

3. **集成确认**: `ProductionQuantSystem.py` 当前未被任何生产代码调用，需确认其是否被动态导入使用，或是否需要接入主流程。

4. **模块质量**: 除上述问题外，四个模块整体实现质量较高，设计模式合理（单例、门面、策略模式），线程安全考虑充分（RLock 使用），错误处理以 try/except 为主并配有 fallback 机制。
