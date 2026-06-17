# 核心服务生产模块排查报告

> 生成时间: 2026-05-18  
> 排查范围: position_service.py / risk_service.py / signal_service.py / order_service.py  
> 排查维度: 方法完整性、集成完备性、方法重复/冲突、魔法数字参数化

---

## 1. position_service.py

### 模块总览

| 指标 | 数值 |
|------|------|
| 总行数 | ~1519 行 |
| 类数 | 3 个 (PositionRecord, PositionLimitConfig, PositionService) |
| 方法数 | ~38 个 (含 dataclass 方法、静态方法、私有方法) |
| 模块级函数/类 | 9 个 (含 GreeksExposure, CrossStrategyRiskGuard 等) |

### 方法完整性检查结果

#### PositionRecord (dataclass)
| 方法 | 状态 | 说明 |
|------|------|------|
| `to_dict()` | 已实现 | JSON 序列化 |
| `from_dict()` | 已实现 | 反序列化 |

#### PositionLimitConfig (dataclass)
| 方法 | 状态 | 说明 |
|------|------|------|
| `is_valid()` | 已实现 | 限额有效性检查 |

#### PositionService
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 集成 SelfTradeDetector/NetworkRetryManager/PartialFillHandler/StructuredJsonlLogger |
| `_get_platform_attr` | 已实现 | 静态方法，统一获取平台属性 |
| `_get_instrument_lock` | 已实现 | 按合约分片锁 |
| `on_trade` | 已实现 | 成交回报更新持仓，集成 PartialFillHandler |
| `on_tick` | 已实现 | 实时行情检查止盈止损，跟踪最大浮盈比例 |
| `set_position_limit` | 已实现 | 委托给 RiskService 设置限额 |
| `get_position` | 已实现 | 查询持仓信息（含平均价格计算） |
| `get_net_position` | 已实现 | 净持仓量 |
| `has_position` | 已实现 | 检查是否有持仓 |
| `check_position_limit` | 已实现 | 统一委托 RiskService 检查 |
| `calculate_position_risk` | 已实现 | 简单风险计算 |
| `get_position_info` | 已实现 | UI 展示用持仓信息 |
| `_add_position` | 已实现 | 添加持仓，集成自成交检测、结构化日志 |
| `_get_tp_sl_ratios_by_reason` | 已实现 | 根据开仓理由获取 TP/SL 倍数 |
| `_map_reason_to_strategy` | 已实现 | 静态方法，理由映射策略 |
| `_apply_crm_stop_loss_adjustment` | 已实现 | 调用 cycle_resonance_module 调整 SL |
| `_get_open_reason_from_order` | 已实现 | 从 OrderService 获取开仓理由 |
| `_reduce_position` | 已实现 | FIFO 平仓 |
| `_check_stop_profit` | 已实现 | 止盈检查 |
| `_check_stop_loss` | 已实现 | 止损检查 |
| `check_trailing_stop` | 已实现 | 浮动止盈检查 |
| `_trigger_close_position` | 已实现 | 触发平仓下单，集成网络重试管理器 |
| `_schedule_close_retry` | 已实现 | 非阻塞延迟重试平仓（ThreadPoolExecutor） |
| `check_all_positions` | 已实现 | 检查所有持仓（时间止损+EOD+权益更新） |
| `_check_time_stop` | 已实现 | 时间止损+PLR弹性调整+SafetyMetaLayer 硬止损 |
| `_check_eod_close` | 已实现 | 收盘前平仓 |
| `_load_position_configs` | 已实现 | 加载配置，委托 RiskService |
| `_save_position_configs` | 已实现 | 保存配置 |
| `get_status` | 已实现 | 服务状态 |

#### 模块级补充功能
| 函数/类 | 状态 | 说明 |
|---------|------|------|
| `_calc_max_volume_from_capital` | 已实现 | 计算最大可用手数 |
| `_cancel_and_resend` | 已实现 | 取消并重发订单 |
| `_check_available_amount` | 已实现 | 检查可用数量 |
| `GreeksExposure` | 已实现 | dataclass，跨策略 Greeks 敞口聚合 |
| `_is_option_instrument` | 已实现 | 判断是否为期权合约 |
| `_estimate_option_delta/vega/gamma` | 已实现 | 估算期权 Greeks |
| `aggregate_greeks_exposure` | 已实现 | 聚合所有持仓 Greeks |
| `CrossStrategyRiskGuard` | 已实现 | 跨策略分层风控守卫 |
| `get_cross_strategy_risk_guard` | 已实现 | 单例获取 |

**结论**: 所有方法均有完整实现，**无空方法/占位符**。

### 集成完备性

#### 被谁调用 (import + 调用)
| 调用方 | 调用内容 |
|--------|----------|
| `order_service.py` | `get_position_service()`, `aggregate_greeks_exposure()`, `get_cross_strategy_risk_guard()` |
| `risk_service.py` | `get_position_service()` (通过 position_manager 参数传入) |
| `strategy_tick_handler.py` | `get_position_service()` |
| `strategy_ecosystem.py` | `get_position_service()`, `aggregate_greeks_exposure()` |
| `box_spring_strategy.py` | `get_position_service()`, `aggregate_greeks_exposure()` |
| `width_cache.py` | `get_position_service()` |
| `mode_engine.py` | 间接通过 risk_service 关联 |

#### 调用了谁
| 被调用模块 | 调用点 |
|-----------|--------|
| `order_service.py` | `get_order_service()` (on_trade 部分成交撤单、_get_open_reason_from_order、_trigger_close_position、_schedule_close_retry、_cancel_and_resend) |
| `risk_service.py` | `get_safety_meta_layer()` (check_all_positions、_check_time_stop), `RiskService._check_position_limit` (check_position_limit), `set_position_limit` (_load_position_configs) |
| `data_service.py` | `get_data_service()` (_trigger_close_position 获取对手价) |
| `params_service.py` | `get_params_service()` (_check_time_stop、_trigger_close_position、_check_eod_close) |
| `config_service.py` | `get_config()` (_load_position_configs) |
| `state_param_manager.py` | `get_state_param_manager()` (_get_tp_sl_ratios_by_reason) |
| `health_check_api.py` | `StructuredJsonlLogger` (__init__) |
| `order_persistence.py` | `SelfTradeDetector`, `NetworkRetryManager`, `PartialFillHandler`, `OrderRecord` (__init__, _add_position) |
| `t_type_service.py` | `get_t_type_service()` (__init__) |
| `参数池.cycle_resonance_module` | `get_cycle_resonance_module()` (_apply_crm_stop_loss_adjustment, _check_time_stop) |

### 方法重复/冲突（与其他模块对比）

| 方法/逻辑 | 所在模块 | 冲突/重复说明 |
|-----------|----------|---------------|
| `set_position_limit` | position_service.py ↔ risk_service.py | **循环委托风险**: PositionService.set_position_limit 委托给 RiskService.set_position_limit，但 RiskService.set_position_limit 又反向委托给 position_manager.set_position_limit。实际运行中 RiskService 的 position_manager 就是 PositionService，可能形成循环调用。 |
| `get_position_limit` | risk_service.py | RiskService.get_position_limit 委托给 position_manager.get_position_limit，但 PositionService 中无此方法定义，调用将失败。 |
| `_get_platform_attr` | position_service.py ↔ order_service.py | OrderService 也有类似逻辑 `_get_platform_attr`，但实现不同（OrderService 只试两个属性名）。**轻度重复**，建议统一。 |
| `generate_daily_signal_report` | signal_service.py ↔ order_service.py | OrderService 也有同名方法，但内容不同（OrderService 报告资金不足信号）。**命名冲突**，功能不同。 |
| `_check_self_trade` | position_service.py (__init__ 集成 SelfTradeDetector) ↔ order_service.py (_check_self_trade) | 两者都有自成交检测逻辑，OrderService 是简单反向挂单检查，PositionService 使用 SelfTradeDetector 类。**重复但层级不同**。 |
| `check_position_limit` | position_service.py ↔ risk_service.py | PositionService 委托给 RiskService._check_position_limit，逻辑单一，无冲突。 |
| `_map_reason_to_strategy` | position_service.py | 模块内出现两次：PositionService 的静态方法和模块级字典 `_REASON_STRATEGY_MAP`。**重复定义**。 |

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 199 | `max_retries=3` | NetworkRetryManager | 已参数化（构造函数参数） |
| 199 | `base_interval_sec=2.0` | NetworkRetryManager | 已参数化 |
| 200 | `timeout_sec=300.0` | PartialFillHandler | 已参数化 |
| 276 | `"0"` | Direction 转换 | 平台协议常量，可保留 |
| 277 | `"0"` | OffsetFlag 转换 | 平台协议常量，可保留 |
| 373 | `24` | 默认有效小时数 | **应参数化**: 影响策略逻辑的限额有效期 |
| 376 | `720` | 最大有效小时数 | **应参数化**: 配置范围限制 |
| 549 | `3` | "开仓超过3天" | UI 展示阈值，可保留 |
| 594 | `1000` | 时间戳毫秒乘数 | 技术常量，可保留 |
| 606 | `1.5` | 默认 tp_ratio | **已部分参数化**: 有 `_FALLBACK_TP_SL` 常量，但建议从配置读取 |
| 607 | `0.5` | 默认 sl_ratio | **已部分参数化**: 同上 |
| 652-658 | 多组 (1.5,0.5) 等 | `_HARDCODED_REASON_DEFAULTS` | **建议参数化**: 硬编码的止盈止损倍数，虽有 YAML 降级，但默认值应可配置 |
| 715 | `0.01, 0.99` | np.clip 范围 | 数学边界常量，可保留 |
| 831 | `2.0` | target_plr 默认值 | **应参数化**: 影响 trailing stop 逻辑 |
| 832 | `1.5` | take_profit_ratio 默认值 | **应参数化**: 影响 trailing stop 逻辑 |
| 833 | `0.5` | target_profit_pct = tp_ratio * 0.5 | **应参数化**: 浮动止盈触发阈值比例 |
| 850 | `0.2` | trailing_stop_pct = peak_profit * 0.2 | **应参数化**: 回撤比例，核心策略参数 |
| 963 | `3` | 重试次数 | **应参数化**: `_schedule_close_retry` 中硬编码 range(1,4) |
| 964 | `0.1` | 基础延迟秒数 | **应参数化**: 重试延迟基数 |
| 1019 | `120` | max_hold_minutes 默认值 | **已部分参数化**: 有 params_service 读取，但默认值硬编码 |
| 1046-1053 | `1.5, 1.2, 0.6, 0.8` | PLR 弹性调整倍数 | **应参数化**: 核心策略参数 |
| 1099 | `14, 55` | eod_close_hour/minute | **已部分参数化**: 有 params_service 读取，但默认值硬编码 |
| 1140 | `"%Y-%m-%d %H:%M:%S"` | 日期格式 | 常量，可保留 |
| 1262 | `1.0` | leverage 默认值 | **应参数化**: 杠杆倍数 |
| 1346 | `0.5` | 期权 delta_per_lot | **应参数化**: 期权 Greeks 估算参数 |
| 1357 | `0.15` | 期权 vega_per_lot | **应参数化**: 同上 |
| 1364 | `0.05` | 期权 gamma_per_lot | **应参数化**: 同上 |
| 1437-1439 | `3.0, 1.5, 0.5` | CrossStrategyRiskGuard 默认限额 | **已参数化**: 构造函数参数 |
| 1465 | `3.0` | 日回撤熔断阈值 | **应参数化**: 核心风控参数 |
| 1469 | `2.0` | 熔断线倍数 | **应参数化**: 风控分级倍数 |
| 1473 | `1.5` | 硬阻断线倍数 | **应参数化**: 同上 |
| 1485 | `1.2` | 预警线倍数 | **应参数化**: 同上 |

### 该模块排查结论

- **方法完整性**: 优秀。所有方法均有完整实现，无空方法或占位符。
- **集成完备性**: 良好。被多个核心模块调用，依赖关系清晰。但存在 `set_position_limit` 与 RiskService 的**循环委托风险**。
- **方法重复/冲突**: 存在 `_map_reason_to_strategy` 模块内重复定义、`get_position_limit` 在 RiskService 中调用但 PositionService 未实现的问题。
- **魔法数字**: 较多硬编码的策略参数（PLR 调整倍数、trailing stop 回撤比例、期权 Greeks 估算值）建议参数化。

---

## 2. risk_service.py

### 模块总览

| 指标 | 数值 |
|------|------|
| 总行数 | ~1451 行 |
| 类数 | 5 个 (RiskLevel, RiskCheckResult, RiskCheckResponse, RiskService, SafetyMetaLayer) |
| 方法数 | ~42 个 |
| 数据结构 | 3 个 (PositionLimit, RiskMetrics, 辅助函数) |

### 方法完整性检查结果

#### RiskService
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化风控参数、限额、限频、连亏保护等 |
| `check_before_trade` | 已实现 | 主检查接口，串联所有风控子检查 |
| `_check_safety_meta_layer` | 已实现 | 安全元层检查 |
| `_record_result` | 已实现 | 记录检查结果统计 |
| `_check_strategy_status` | 已实现 | 策略运行状态检查（带缓存） |
| `_check_rate_limit` | 已实现 | 信号限频检查（滑动窗口） |
| `_record_signal_time` | 已实现 | 记录信号时间 |
| `_check_position_limit` | 已实现 | 持仓限额检查 |
| `set_position_limit` | 已实现 | **反向委托给 PositionService** |
| `get_position_limit` | 已实现 | **反向委托给 PositionService** |
| `_check_risk_ratio` | 已实现 | 风险比率检查 |
| `_get_greeks_calculator` | 已实现 | 懒加载 GreeksCalculator |
| `get_greeks_dashboard` | 已实现 | V7 核心：实时希腊字母仪表盘 |
| `_compute_stress_test` | 已实现 | 跳空模拟压力测试 |
| `_compute_pnl_attribution` | 已实现 | PnL 归因分析 |
| `log_greeks_dashboard_periodic` | 已实现 | 周期性仪表盘日志 |
| `_check_greeks_limits` | 已实现 | 希腊字母硬约束检查 |
| `calculate_risk_metrics` | 已实现 | 计算风险指标 |
| `_get_signal_cooldown` | 已实现 | 读取信号冷却时间 |
| `_get_max_signals_per_window` | 已实现 | 读取最大信号数 |
| `_get_rate_limit_window` | 已实现 | 读取限频窗口 |
| `_get_max_risk_ratio` | 已实现 | 读取最大风险比率 |
| `_get_total_position_limit` | 已实现 | 读取总持仓限额 |
| `get_stats` | 已实现 | 统一统计接口 |
| `reset_stats` | 已实现 | 重置统计 |
| `clear_cache` | 已实现 | 清除缓存 |
| `_check_consecutive_loss_protection` | 已实现 | 连亏保护检查 |
| `record_trade_result` | 已实现 | 记录交易结果，更新连亏计数 |
| `reset_consecutive_loss` | 已实现 | 手动重置连亏状态 |
| `set_capital_scale` | 已实现 | 设置资金规模，联动非对称风控 |
| `compute_mode_position_size` | 已实现 | 委托 ModeEngine 计算仓位 |
| `calculate_asymmetric_drawdown_limit` | 已实现 | 非对称回撤限额计算 |
| `check_asymmetric_risk` | 已实现 | 非对称风控检查 |
| `compute_decision_score` | 已实现 | 决策评分计算 |

#### SafetyMetaLayer
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化安全元层状态 |
| `on_equity_update` | 已实现 | 权益更新，触发断路器和日回撤检查 |
| `_check_circuit_breaker` | 已实现 | 速率断路器检查（1分钟回撤 > 2.5σ） |
| `_check_daily_drawdown` | 已实现 | 日最大回撤硬停止检查 |
| `check_position_hard_time_stop` | 已实现 | 持仓时间硬止损检查（两阶段） |
| `is_trading_paused` | 已实现 | 检查交易是否暂停 |
| `is_new_open_blocked` | 已实现 | 检查是否禁止新开仓 |
| `is_hard_stop_triggered` | 已实现 | 检查硬停止是否触发 |
| `confirm_daily_resume` | 已实现 | 人工确认恢复交易 |
| `set_prev_5day_avg_profit` | 已实现 | 设置前5日平均收益 |
| `_get_circuit_breaker_pause_sec` | 已实现 | 读取断路器暂停时长 |
| `_get_circuit_breaker_calm_period_sec` | 已实现 | 读取冷静期时长 |
| `_cancel_pending_on_circuit_breaker` | 已实现 | 断路器触发时撤销挂单 |
| `_get_daily_drawdown_multiplier` | 已实现 | 读取日回撤乘数 |
| `_get_hard_time_stop_minutes` | 已实现 | 读取硬止损分钟数 |
| `_get_min_profit_threshold` | 已实现 | 读取最小盈利阈值 |
| `_get_stage1_minutes` | 已实现 | 读取阶段1分钟数 |
| `_get_stage2_minutes` | 已实现 | 读取阶段2分钟数 |
| `_get_stage1_profit_threshold` | 已实现 | 读取阶段1盈利阈值 |
| `get_stats` | 已实现 | 获取统计 |

**结论**: 所有方法均有完整实现，**无空方法/占位符**。

### 集成完备性

#### 被谁调用
| 调用方 | 调用内容 |
|--------|----------|
| `position_service.py` | `get_safety_meta_layer()`, `RiskService._check_position_limit`, `set_position_limit`, `PositionLimit` |
| `signal_service.py` | `get_risk_service()`, `compute_decision_score()` |
| `order_service.py` | `get_cross_strategy_risk_guard()`, `aggregate_greeks_exposure()` (通过 position_service) |
| `strategy_ecosystem.py` | `get_risk_service()` |
| `box_spring_strategy.py` | `get_risk_service()` |
| `mode_engine.py` | `get_risk_service()` |
| `strategy_lifecycle_mixin.py` | `get_safety_meta_layer()` |

#### 调用了谁
| 被调用模块 | 调用点 |
|-----------|--------|
| `order_service.py` | `get_order_service()` (_cancel_pending_on_circuit_breaker) |
| `position_service.py` | `set_position_limit`, `get_position_limit` (反向委托) |
| `greeks_calculator.py` | `GreeksCalculator` (_get_greeks_calculator) |
| `data_service.py` | `get_data_service()` (_compute_stress_test) |
| `params_service.py` | 间接通过 safe_get_float 读取参数 |
| `governance_engine.py` | `E7UnexplainedReturnChecker` (log_greeks_dashboard_periodic) |
| `state_param_manager.py` | `get_state_param_manager()` (set_capital_scale) |
| `mode_engine.py` | `ModeEngine` (compute_mode_position_size) |

### 方法重复/冲突

| 方法/逻辑 | 所在模块 | 冲突/重复说明 |
|-----------|----------|---------------|
| `set_position_limit` / `get_position_limit` | risk_service.py ↔ position_service.py | **循环委托**: RiskService 将限额操作反向委托给 position_manager (即 PositionService)，而 PositionService 又委托给 RiskService。实际 PositionService 未实现 `get_position_limit`，调用会失败。 |
| `compute_decision_score` | risk_service.py ↔ signal_service.py | SignalService.apply_decision_score_filter 调用 RiskService.compute_decision_score，无重复定义。 |
| `get_stats` | 所有四个服务 | 各服务均有 `get_stats()` 方法，返回统一格式（含 service_name），**这是设计意图，非冲突**。 |
| `clear_cache` | risk_service.py | 仅 RiskService 有，无冲突。 |
| `safe_get_float/safe_get_int` | risk_service.py | 模块级辅助函数，被广泛使用，无冲突。 |

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 167 | `5, 7, 10` | 连亏限制次数 (small/medium/large) | **应参数化**: 核心风控参数 |
| 173-175 | `1800.0, 3600.0, 7200.0` | 连亏恢复超时（秒） | **应参数化**: 风控恢复时间 |
| 181 | `1.5` | 非对称盈利乘数 | **应参数化**: 非对称风控参数 |
| 182 | `0.7` | 非对称亏损乘数 | **应参数化**: 同上 |
| 186 | `1.0` | 策略状态缓存 TTL | **应参数化**: 缓存有效期 |
| 338 | `maxlen=max_signals * 2` | 信号时间窗口 maxlen | 实现细节，可保留 |
| 521 | `0.30` | max_net_delta_pct 默认值 | **应参数化**: Greeks 限额 |
| 522 | `0.08` | max_net_gamma_pct 默认值 | **应参数化**: 同上 |
| 523 | `0.02` | max_net_vega_bps 默认值 | **应参数化**: 同上 |
| 524 | `0.5` | max_theta_ratio 默认值 | **应参数化**: 同上 |
| 534 | `1000` | max_delta_abs 计算基数 | 业务常量，可保留 |
| 535 | `50` | max_gamma_abs 计算基数 | 业务常量，可保留 |
| 536 | `200` | max_vega_abs 计算基数 | 业务常量，可保留 |
| 605-606 | `0.05, 0.01` | L1_JUMP_PROB_HIGH/LOW | **应参数化**: 压力测试参数 |
| 625 | `0.2` | 默认 IV | **应参数化**: 波动率假设 |
| 640 | `0.01` | sigma_1 计算系数 | 数学公式常量，可保留 |
| 683 | `0.01` | delta_contrib 价格变动 | 数学公式常量，可保留 |
| 684 | `0.0001` | gamma_contrib 价格变动 | 数学公式常量，可保留 |
| 685 | `iv * 0.01` | vega_contrib IV变动 | 数学公式常量，可保留 |
| 693 | `15.0` | 未解释收益告警阈值 | **应参数化**: E7 告警阈值 |
| 727-730 | `100, 15` | usage_warning 阈值 | UI 告警阈值，可保留 |
| 765 | `15` | E7 阈值 | **应参数化**: 与 693 行重复定义 |
| 876 | `1.0` | signal_cooldown_sec 默认值 | **应参数化**: 信号冷却 |
| 881 | `10` | max_signals_per_window 默认值 | **应参数化**: 限频参数 |
| 885 | `60.0` | rate_limit_window_sec 默认值 | **应参数化**: 限频窗口 |
| 889 | `0.8` | max_risk_ratio 默认值 | **应参数化**: 风险比率 |
| 893 | `1000000.0` | total_position_limit 默认值 | **应参数化**: 总持仓限额 |
| 1007 | `0.02` | compute_mode_position_size 默认 risk_pct | **应参数化**: 仓位计算风险比例 |
| 1015 | `1e-10` | stop_distance 最小值 | 数学防除零，可保留 |
| 1046 | `0.6, 0.4` | decision_score 权重 | **应参数化**: 决策评分权重 |
| 1047 | `0.7, 0.3` | state_strength/order_flow_consistency 阈值 | **应参数化**: 决策阈值 |
| 1054 | `0.4, 0` | state_strength 第二档阈值 | **应参数化**: 同上 |
| 1118 | `2.5` | ANOMALY_THRESHOLD_MULTIPLIER | **已类常量**: 可回测优化参数 |
| 1119 | `0.05` | DEFAULT_ANOMALY_THRESHOLD | **已类常量**: 同上 |
| 1120 | `0.05` | DEFAULT_MAX_DRAWDOWN | **已类常量**: 同上 |
| 1126-1127 | `60` | equity_series maxlen | 技术常量，可保留 |
| 1190 | `6` | one_min_ago_idx 计算 | 技术常量（假设 10s 间隔，6=1min），可保留 |
| 1211 | `0.02` | 断路器最小阈值 | **应参数化**: 最小回撤触发阈值 |
| 1248 | `5%%` | 默认日回撤硬限 | **已类常量**: 与 DEFAULT_MAX_DRAWDOWN 一致 |
| 1280 | `90.0, 240.0` | stage1/stage2 分钟数 | **应参数化**: 硬止损时间阶段 |
| 1282 | `0.002` | stage1 盈利阈值 | **应参数化**: 硬止损盈利要求 |
| 1306 | `0.5` | 阶段2回撤超50% | **应参数化**: 回撤比例阈值 |
| 1350 | `180.0` | circuit_breaker_pause_sec 默认值 | **应参数化**: 断路器暂停时长 |
| 1353 | `600.0` | circuit_breaker_calm_period_sec 默认值 | **应参数化**: 冷静期时长 |
| 1367 | `2.0` | daily_drawdown_multiplier 默认值 | **应参数化**: 日回撤乘数 |
| 1370 | `90.0` | hard_time_stop_minutes 默认值 | **应参数化**: 硬止损时间 |
| 1373 | `0.002` | min_profit_threshold 默认值 | **应参数化**: 最小盈利阈值 |

### 该模块排查结论

- **方法完整性**: 优秀。所有方法均有完整实现，无空方法或占位符。
- **集成完备性**: 良好。被多个策略模块调用。但 `set_position_limit` / `get_position_limit` 与 PositionService 存在**循环委托设计缺陷**，且 PositionService 缺少 `get_position_limit` 方法。
- **方法重复/冲突**: 主要是与 PositionService 的限额管理接口设计问题。
- **魔法数字**: 大量风控参数硬编码（连亏阈值、Greeks 限额、决策评分阈值、断路器参数），建议统一从配置读取。

---

## 3. signal_service.py

### 模块总览

| 指标 | 数值 |
|------|------|
| 总行数 | ~760 行 |
| 类数 | 5 个 (SignalService, KalmanFilter1D, EMASignalFilter, SignalTimingFilter, AdaptiveSignalThreshold) |
| 方法数 | ~28 个 |

### 方法完整性检查结果

#### SignalService
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化信号历史、冷却、统计、EventBus、StructuredJsonlLogger |
| `enable_hft_filter` | 已实现 | 启用 HFT 信号时序滤波 |
| `enable_plr_filter` | 已实现 | 启用 PLR 过滤 |
| `disable_plr_filter` | 已实现 | 禁用 PLR 过滤 |
| `filter_with_hft` | 已实现 | HFT 滤波信号 |
| `generate_signal` | 已实现 | 核心信号生成，含 PLR/ModeEngine/冷却过滤 |
| `_is_in_cooldown` | 已实现 | 冷却检查 |
| `get_signal_history` | 已实现 | 获取信号历史 |
| `set_cooldown` | 已实现 | 设置冷却 |
| `clear_cooldown` | 已实现 | 清除冷却 |
| `reset_signal_history` | 已实现 | 重置信号历史 |
| `run_benchmark` | 已实现 | 性能基准测试 |
| `get_stats` | 已实现 | 统一统计接口（含定期清理） |
| `validate_signal` | 已实现 | 信号有效性验证 |
| `generate_daily_signal_report` | 已实现 | 日信号报告生成 |
| `check_market_close_and_report` | 已实现 | 收盘检查并生成报告 |
| `apply_decision_score_filter` | 已实现 | 应用决策评分过滤 |

#### KalmanFilter1D
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化卡尔曼滤波参数 |
| `update` | 已实现 | 更新测量值 |
| `get_state` | 已实现 | 获取当前状态 |
| `reset` | 已实现 | 重置滤波器 |

#### EMASignalFilter
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化 EMA 参数 |
| `update` | 已实现 | 更新信号值 |
| `is_bullish_crossover` | 已实现 | 判断金叉 |
| `get_state` | 已实现 | 获取状态 |

#### SignalTimingFilter
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化滤波器组合 |
| `filter_signal` | 已实现 | 信号滤波主逻辑 |
| `get_stats` | 已实现 | 统计信息 |

#### AdaptiveSignalThreshold
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化自适应阈值 |
| `threshold` (property) | 已实现 | 当前阈值 |
| `record_signal` | 已实现 | 记录信号结果 |
| `_adapt` | 已实现 | 自适应调整阈值 |
| `get_stats` | 已实现 | 统计信息 |

**结论**: 所有方法均有完整实现，**无空方法/占位符**。

### 集成完备性

#### 被谁调用
| 调用方 | 调用内容 |
|--------|----------|
| `mode_engine.py` | `get_signal_service()` |
| `strategy_ecosystem.py` | `SignalService`, `get_signal_service()` |
| `strategy_tick_handler.py` | `SignalService` (直接实例化) |
| `hft_enhancements.py` | `SignalService`, `SignalTimingFilter` |

#### 调用了谁
| 被调用模块 | 调用点 |
|-----------|--------|
| `risk_service.py` | `get_risk_service()` (apply_decision_score_filter) |
| `mode_engine.py` | `ModeEngine.get_instance()` (generate_signal) |
| `event_bus.py` | `EventBus`, `SignalEvent`, `get_global_event_bus()` (__init__, generate_signal) |
| `health_check_api.py` | `StructuredJsonlLogger` (__init__) |

### 方法重复/冲突

| 方法/逻辑 | 所在模块 | 冲突/重复说明 |
|-----------|----------|---------------|
| `generate_daily_signal_report` | signal_service.py ↔ order_service.py | **命名冲突**: OrderService 也有同名方法，但内容完全不同（OrderService 报告资金不足信号）。建议重命名区分。 |
| `get_stats` | 所有四个服务 | 统一设计，非冲突。 |
| `check_market_close_and_report` | signal_service.py | 仅 SignalService 有，无冲突。 |
| `apply_decision_score_filter` | signal_service.py | 调用 RiskService.compute_decision_score，无重复定义。 |

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 54 | `1000` | SIGNAL_HISTORY_MAX_LEN | **应参数化**: 信号历史长度限制 |
| 55 | `60.0` | DEFAULT_COOLDOWN_SECONDS | **应参数化**: 默认冷却时间 |
| 56 | `300` | CLEANUP_INTERVAL_SECONDS | 技术常量，可保留 |
| 111 | `0.6` | HFT filter threshold 默认值 | **已参数化**: 构造函数参数 |
| 116 | `2.0` | PLR filter min_estimated_plr 默认值 | **已参数化**: 方法参数 |
| 142 | `999` | days_to_expiry 默认值 | **应参数化**: 到期日默认值 |
| 158 | `('BUY', 'SELL', 'CLOSE_LONG', 'CLOSE_SHORT')` | 有效信号类型 | 业务常量，可保留 |
| 197 | `12` | UUID 截取长度 | 技术常量，可保留 |
| 349 | `3500.0` | 基准测试基准价格 | 测试常量，可保留 |
| 404 | `self._default_cooldown_seconds` | 过期冷却清理阈值 | 使用已有配置，合理 |
| 444 | `0` | 价格和手数最小值 | 业务校验，可保留 |
| 537 | `15, 15` | 收盘时间 15:15 | **应参数化**: 市场收盘时间 |
| 537 | `15, 20` | 检查结束时间 15:20 | **应参数化**: 同上 |
| 578 | `1e-4, 1e-2` | KalmanFilter1D 默认方差 | **已参数化**: 构造函数参数 |
| 617 | `5, 20` | EMA fast/slow period | **已参数化**: 构造函数参数 |
| 651 | `0.6` | SignalTimingFilter threshold | **已参数化**: 构造函数参数 |
| 651 | `1e-4, 1e-2` | Kalman 方差 | **已参数化**: 构造函数参数 |
| 651 | `5, 20` | EMA period | **已参数化**: 构造函数参数 |
| 720 | `0.3, 0.15, 0.6, 0.4` | AdaptiveSignalThreshold 默认值 | **已参数化**: 构造函数参数 |
| 729 | `100, 50` | 近期记录 maxlen | 技术常量，可保留 |
| 740 | `20` | 自适应调整最小样本数 | **应参数化**: 调整触发阈值 |
| 746 | `10` | PnL 样本最小数 | **应参数化**: 同上 |
| 749 | `0.5` | 亏损额外调整系数 | **应参数化**: 自适应速率 |

### 该模块排查结论

- **方法完整性**: 优秀。所有方法均有完整实现，无空方法或占位符。
- **集成完备性**: 良好。被策略生态系统和 tick 处理器调用。与 RiskService 通过决策评分关联。
- **方法重复/冲突**: 仅 `generate_daily_signal_report` 与 OrderService 命名冲突。
- **魔法数字**: 相对较少，大部分已通过构造函数参数化。默认冷却时间、信号历史长度、市场收盘时间建议参数化。

---

## 4. order_service.py

### 模块总览

| 指标 | 数值 |
|------|------|
| 总行数 | ~1421 行 |
| 类数 | 7 个 (BaseService, PlatformAuthenticator, OrderService, OrderSplitStrategy, SplitOrderResult, SmartOrderSplitter, IcebergOrderSplitter, TWAPSplitter) |
| 方法数 | ~45 个 |

### 方法完整性检查结果

#### PlatformAuthenticator
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化 token 状态 |
| `get_token` | 已实现 | 获取有效 token |
| `set_token` | 已实现 | 设置 token 及过期时间 |

#### OrderService
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化订单状态、统计、HFT 组件 |
| `bind_platform_apis` | 已实现 | 绑定平台下单/撤单 API |
| `_get_platform_attr` | 已实现 | 获取平台属性（双属性名） |
| `_normalize_platform_result` | 已实现 | 规范化平台返回结果 |
| `send_order` | 已实现 | 主下单接口，含限流/自成交/跨策略风控检查 |
| `_execute_send_order` | 已实现 | 执行平台下单调用 |
| `_retry_send_order` | 已实现 | 指数退避重试 |
| `_check_cross_strategy_risk` | 已实现 | 跨策略 Greeks 风控检查 |
| `_check_self_trade` | 已实现 | 自成交检测 |
| `execute_by_ranking` | 已实现 | 按排名批量下单 |
| `cancel_order` | 已实现 | 撤单 |
| `cancel_all_pending` | 已实现 | 撤销所有未成交订单 |
| `on_trade_update` | 已实现 | 成交更新处理，含部分成交检测 |
| `check_pending_orders` | 已实现 | 检查超时订单并追单 |
| `_get_tick_size` | 已实现 | 获取合约最小变动价位 |
| `_chase_reorder` | 已实现 | 追单重发 |
| `get_order` | 已实现 | 查询订单 |
| `get_orders_by_instrument` | 已实现 | 按合约查询订单 |
| `get_stats` | 已实现 | 统一统计接口 |
| `_cleanup_orders` | 已实现 | 独立订单清理方法 |
| `_is_duplicate_order` | 已实现 | 重复订单检查 |
| `_generate_order_id` | 已实现 | 生成订单 ID |
| `_get_rate_limit` | 已实现 | 读取全局速率限制 |
| `_ensure_trade_log` | 已实现 | 初始化交易日志 |
| `_persist_trade_log` | 已实现 | 持久化交易日志 |
| `persist_close_event` | 已实现 | 平仓事件持久化 |
| `_record_insufficient_fund` | 已实现 | 记录资金不足信号 |
| `record_insufficient_fund_from_log` | 已实现 | 外部资金不足信号记录 |
| `_create_virtual_position` | 已实现 | 创建虚拟仓位 |
| `_calc_pnl` | 已实现 | 计算盈亏 |
| `mark_virtual_positions_eod` | 已实现 | 虚拟仓位隔夜标记 |
| `on_close_signal` | 已实现 | 平仓信号处理 |
| `get_virtual_positions` | 已实现 | 获取虚拟仓位 |
| `get_virtual_position_summary` | 已实现 | 虚拟仓位汇总 |
| `generate_daily_signal_report` | 已实现 | 日信号报告（资金不足信号） |
| `get_insufficient_fund_signals` | 已实现 | 获取资金不足信号 |
| `enable_hft_enhancements` | 已实现 | 启用 HFT 增强 |
| `send_order_split` | 已实现 | HFT 拆单下单 |
| `send_defensive_order` | 已实现 | HFT 防御性下单 |

#### SmartOrderSplitter
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化拆单参数 |
| `plan_order_split` | 已实现 | 规划订单拆分 |
| `_resolve_strategy` | 已实现 | 解析拆单策略 |
| `_plan_aggressive_split` | 已实现 | 激进拆单 |
| `_plan_passive_split` | 已实现 | 被动拆单 |
| `_plan_adaptive_split` | 已实现 | 自适应拆单 |
| `_estimate_slippage` | 已实现 | 估算滑点 |
| `get_stats` | 已实现 | 统计信息 |

#### IcebergOrderSplitter
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化冰山参数 |
| `split` | 已实现 | 冰山拆单 |

#### TWAPSplitter
| 方法 | 状态 | 说明 |
|------|------|------|
| `__init__` | 已实现 | 初始化 TWAP 参数 |
| `split` | 已实现 | TWAP 时间拆分 |

**结论**: 所有方法均有完整实现，**无空方法/占位符**。

### 集成完备性

#### 被谁调用
| 调用方 | 调用内容 |
|--------|----------|
| `position_service.py` | `get_order_service()` (多处：on_trade 部分成交撤单、_trigger_close_position、_schedule_close_retry、_cancel_and_resend) |
| `risk_service.py` | `get_order_service()` (_cancel_pending_on_circuit_breaker) |
| `strategy_tick_handler.py` | `get_order_service()` |
| `strategy_ecosystem.py` | 间接通过 position_service |
| `box_spring_strategy.py` | `get_order_service()` (多处) |
| `hft_enhancements.py` | `OrderService`, `get_order_service()` |

#### 调用了谁
| 被调用模块 | 调用点 |
|-----------|--------|
| `position_service.py` | `get_position_service()`, `aggregate_greeks_exposure()`, `get_cross_strategy_risk_guard()` (_check_cross_strategy_risk) |
| `config_service.py` | `resolve_product_exchange()` (_execute_send_order) |
| `params_service.py` | `get_params_service()` (_get_tick_size, _calc_pnl) |
| `data_service.py` | `get_data_service()` (mark_virtual_positions_eod, on_close_signal) |
| `order_flow_bridge.py` | `MarketMakerDefenseEngine` (send_defensive_order) |
| `event_bus.py` | `RateLimiter` (__init__) |

### 方法重复/冲突

| 方法/逻辑 | 所在模块 | 冲突/重复说明 |
|-----------|----------|---------------|
| `generate_daily_signal_report` | order_service.py ↔ signal_service.py | **命名冲突**: 两者同名但功能不同。OrderService 报告资金不足信号，SignalService 报告交易信号。建议 OrderService 重命名为 `generate_daily_insufficient_fund_report`。 |
| `_get_platform_attr` | order_service.py ↔ position_service.py | **轻度重复**: 两者都用于兼容不同平台的属性名，但实现不同。建议统一工具函数。 |
| `_check_self_trade` | order_service.py ↔ position_service.py | **重复但层级不同**: OrderService 做简单反向挂单检查，PositionService 使用 SelfTradeDetector 类做更复杂检测。两者同时存在有一定冗余。 |
| `get_stats` | 所有四个服务 | 统一设计，非冲突。 |
| `_calc_pnl` | order_service.py | 仅 OrderService 有，无冲突。 |

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 39 | `3600.0` | DEFAULT_TOKEN_EXPIRY_SECONDS | **应参数化**: token 过期时间 |
| 74 | `5.0` | DEFAULT_ORDER_TIMEOUT_SECONDS | **应参数化**: 订单超时 |
| 75 | `3` | MAX_CHASE_RETRIES | **应参数化**: 最大追单次数 |
| 76 | `3` | MAX_SEND_RETRIES | **应参数化**: 最大发送重试次数 |
| 77 | `300` | CLEANUP_INTERVAL_SECONDS | 技术常量，可保留 |
| 78 | `100` | DEFAULT_CONTRACT_MULTIPLIER | **应参数化**: 默认合约乘数 |
| 84 | `60.0` | rate_per_min 转 rate_per_sec | 数学转换，可保留 |
| 121 | `1000` | _max_insufficient_fund_signals | **应参数化**: 信号存储上限 |
| 122 | `10000` | _max_pnl_history | **应参数化**: PnL 历史上限 |
| 187 | `round(price, 4)` | 重复订单检查价格精度 | **应参数化**: 价格精度 |
| 309 | `2 ** retry` | 指数退避延迟 | 算法常量，可保留 |
| 370-372 | `price + tick_size / max(0.01, price - tick_size)` | 下单价格调整 | 算法逻辑，tick_size 已参数化 |
| 478 | `300` | 已完成订单清理超时（秒） | **应参数化**: 订单保留时间 |
| 482 | `self._order_timeout_seconds` | 超时判断 | 使用已有配置，合理 |
| 493 | `retry_count < self._max_chase_retries` | 追单次数判断 | 使用已有配置，合理 |
| 516-524 | `0.2` | 股指期货默认 tick_size | **已部分参数化**: 有 prefix 匹配，但值硬编码 |
| 525 | `1.0` | 最终降级 tick_size | **应参数化**: 默认 tick_size |
| 530 | `min(retry_count, 3)` | 追单 tick 数上限 | **应参数化**: 追单价格调整上限 |
| 598-603 | `20, -20` | 重复订单 recent 检查数量 | 技术常量，可保留 |
| 612 | `60` | 默认全局速率限制 | **应参数化**: 默认限流 |
| 616 | `60` | rate_limit_global_per_min 默认值 | **应参数化**: 同上 |
| 725 | `0.0` | theoretical_pnl 默认值 | 业务默认值，可保留 |
| 821 | `100` | _calc_pnl 默认 contract_size | **应参数化**: 默认合约乘数（与 78 行重复） |
| 879 | `-self._max_pnl_history:` | PnL 历史截断 | 使用已有配置，合理 |
| 1213 | `5` | SmartOrderSplitter max_depth_levels | **已参数化**: 构造函数参数 |
| 1214 | `0.8` | aggressive_signal_threshold | **已参数化**: 构造函数参数 |
| 1215 | `0.6` | passive_signal_threshold | **已参数化**: 构造函数参数 |
| 1216 | `3` | max_slippage_ticks | **已参数化**: 构造函数参数 |
| 1217 | `3.0` | high_plr_threshold | **已参数化**: 构造函数参数 |
| 1218 | `1.5` | low_plr_threshold | **已参数化**: 构造函数参数 |
| 1293 | `1` | aggressive_split price_offset_ticks | 拆单默认值，可保留 |
| 1325-1326 | `1.0, 0.01` | adaptive_split ratio 计算 | 算法常量，可保留 |
| 1346 | `0.2` | _estimate_slippage 默认 tick_size | **应参数化**: 默认 tick_size |
| 1362 | `5` | IcebergOrderSplitter avg_display_volume | **已参数化**: 构造函数参数 |
| 1363 | `0.5` | randomize_factor | **已参数化**: 构造函数参数 |
| 1392 | `10` | TWAPSplitter num_slices | **已参数化**: 构造函数参数 |
| 1393 | `60.0` | TWAPSplitter time_window_seconds | **已参数化**: 构造函数参数 |
| 1394 | `True` | randomize_timing | **已参数化**: 构造函数参数 |
| 1412 | `0.3` | TWAP jitter 系数 | **应参数化**: 时间抖动比例 |

### 该模块排查结论

- **方法完整性**: 优秀。所有方法均有完整实现，无空方法或占位符。
- **集成完备性**: 良好。被 PositionService 和多个策略模块调用。依赖 PositionService 做跨策略风控。
- **方法重复/冲突**: `generate_daily_signal_report` 与 SignalService 命名冲突；`_get_platform_attr` 与 PositionService 轻度重复。
- **魔法数字**: 订单超时、重试次数、默认合约乘数等建议参数化。HFT 拆单参数已通过构造函数参数化，设计良好。

---

## 跨模块方法重复/冲突汇总

| # | 问题 | 涉及模块 | 严重程度 | 建议修复 |
|---|------|----------|----------|----------|
| 1 | `set_position_limit` 循环委托 | position_service.py ↔ risk_service.py | **高** | 明确唯一权威源：建议以 RiskService 为唯一限额存储源，PositionService 仅提供入口委托，RiskService 不再反向委托。 |
| 2 | `get_position_limit` 调用未实现方法 | risk_service.py → position_service.py | **高** | PositionService 缺少 `get_position_limit` 方法，需补充实现，或 RiskService 直接使用本地 `_position_limits`。 |
| 3 | `generate_daily_signal_report` 命名冲突 | signal_service.py ↔ order_service.py | 中 | OrderService 方法重命名为 `generate_daily_insufficient_fund_report` 或 `generate_daily_order_report`。 |
| 4 | `_get_platform_attr` 轻度重复 | position_service.py ↔ order_service.py | 低 | 提取为公共工具函数，统一放在 `compat_utils.py` 或类似模块。 |
| 5 | `_check_self_trade` 重复逻辑 | position_service.py ↔ order_service.py | 低 | OrderService 保留快速检查，PositionService 保留 SelfTradeDetector 深度检查，两者互补，可保留。 |
| 6 | `_map_reason_to_strategy` 模块内重复 | position_service.py | 低 | 删除 PositionService 的静态方法，统一使用模块级 `_REASON_STRATEGY_MAP` 字典。 |
| 7 | `get_stats` 统一接口 | 四个服务 | 无 | 设计意图，保持现状。 |

---

## 魔法数字参数化优先级汇总

### 高优先级（影响策略逻辑，必须参数化）

| 模块 | 行号 | 数字 | 说明 |
|------|------|------|------|
| position_service.py | 652-658 | tp/sl 倍数 (1.5, 0.5 等) | `_HARDCODED_REASON_DEFAULTS` 默认值 |
| position_service.py | 833 | 0.5 | trailing stop 触发比例 |
| position_service.py | 850 | 0.2 | trailing stop 回撤比例 |
| position_service.py | 1046-1053 | 1.5, 1.2, 0.6, 0.8 | PLR 弹性调整倍数 |
| position_service.py | 1346,1357,1364 | 0.5, 0.15, 0.05 | 期权 Greeks 估算参数 |
| risk_service.py | 167 | 5, 7, 10 | 连亏限制次数 |
| risk_service.py | 173-175 | 1800, 3600, 7200 | 连亏恢复超时 |
| risk_service.py | 181-182 | 1.5, 0.7 | 非对称风控乘数 |
| risk_service.py | 1046-1054 | 0.6, 0.4, 0.7, 0.3, 0.4 | 决策评分权重和阈值 |
| risk_service.py | 1211 | 0.02 | 断路器最小阈值 |
| risk_service.py | 1280-1282 | 90, 240, 0.002 | 硬止损阶段参数 |
| risk_service.py | 1306 | 0.5 | 阶段2回撤比例 |
| order_service.py | 74-76 | 5.0, 3, 3 | 订单超时和重试次数 |
| order_service.py | 78, 821 | 100 | 默认合约乘数 |

### 中优先级（建议参数化）

| 模块 | 行号 | 数字 | 说明 |
|------|------|------|------|
| position_service.py | 373, 376 | 24, 720 | 限额有效期默认值和最大值 |
| position_service.py | 1019 | 120 | max_hold_minutes 默认值 |
| position_service.py | 1099 | 14, 55 | EOD 收盘时间 |
| risk_service.py | 186 | 1.0 | 策略状态缓存 TTL |
| risk_service.py | 521-524 | 0.30, 0.08, 0.02, 0.5 | Greeks 限额默认值 |
| risk_service.py | 605-606 | 0.05, 0.01 | L1 跳跃概率 |
| risk_service.py | 625 | 0.2 | 默认 IV |
| risk_service.py | 693, 765 | 15.0 | E7 未解释收益阈值 |
| risk_service.py | 876-893 | 1.0, 10, 60, 0.8, 1000000 | 风控参数默认值 |
| signal_service.py | 54-55 | 1000, 60.0 | 信号历史长度和默认冷却 |
| signal_service.py | 537 | 15, 15 / 15, 20 | 市场收盘时间 |
| order_service.py | 39 | 3600.0 | token 过期时间 |
| order_service.py | 478 | 300 | 已完成订单清理超时 |
| order_service.py | 525 | 1.0 | 最终降级 tick_size |

### 低优先级（技术常量/索引，可保留）

- UUID 截取长度、日期格式字符串、deque maxlen、数学防除零极小值 (1e-10)、平台协议方向常量 ("0"/"1")、EMA/Kalman 数学公式中的固定系数。

---

## 总体结论

1. **方法完整性**: 四个核心服务模块方法实现完整，**无空方法或占位符**，代码质量良好。

2. **集成完备性**: 四个服务形成了较为完整的调用链：
   - SignalService → RiskService (决策评分)
   - RiskService → OrderService (断路器撤单)
   - OrderService → PositionService (跨策略风控)
   - PositionService → OrderService (平仓下单)
   但 `position_service.py` 与 `risk_service.py` 之间的限额管理存在**循环委托设计缺陷**，需要修复。

3. **方法重复/冲突**: 主要问题集中在：
   - PositionService ↔ RiskService 的限额管理接口循环委托
   - PositionService 缺少 `get_position_limit` 方法
   - SignalService ↔ OrderService 的 `generate_daily_signal_report` 命名冲突
   - `_get_platform_attr` 在两个模块中轻度重复

4. **魔法数字**: 
   - `position_service.py` 和 `risk_service.py` 硬编码的策略参数较多，尤其是止盈止损倍数、PLR 调整系数、风控阈值等核心参数需要参数化。
   - `signal_service.py` 和 `order_service.py` 参数化做得较好，大部分通过构造函数或配置读取。

5. **修复建议优先级**:
   - **P0（立即修复）**: 解决 RiskService ↔ PositionService 的 `set_position_limit` 循环委托；补充 PositionService.`get_position_limit` 方法。
   - **P1（尽快修复）**: 将核心策略参数（TP/SL 倍数、trailing stop 比例、PLR 调整系数、连亏阈值）统一参数化。
   - **P2（建议修复）**: 重命名冲突方法；提取公共 `_get_platform_attr` 工具函数。
