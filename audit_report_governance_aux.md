# 生产模块排查报告：治理、订单持久化、健康检查、数据服务、箱体检测、弹簧策略

> 排查日期：2026-05-18
> 排查范围：6个生产模块（governance_engine, order_persistence, health_check_api, data_service, box_detector, box_spring_strategy）
> 排查维度：方法完整性、集成完备性、方法重复/冲突、魔法数字参数化

---

## 1. governance_engine.py

### 模块总览
- **文件路径**：`ali2026v3_trading/governance_engine.py`
- **行数**：302
- **类数**：7
- **方法数**：约 18 个（含 `__init__`）

### 类与方法清单

| 类名 | 方法 | 状态 |
|------|------|------|
| `E12ReverseStrategyPseudoIndependenceDetector` | `__init__`, `detect` | 完整 |
| `E13ShadowStrategyCollusionDetector` | `__init__`, `detect` | 完整 |
| `MultiStateSwitchBacktestScenario` | `__init__`, `run_scenario` | 完整 |
| `WF6ToWF10EliminationChecker` | `__init__`, `_check_wf6_monotone_decline`, `_check_wf7_parameter_fragility`, `_check_wf8_negative_ev`, `_check_wf9_alpha_decline`, `_check_wf10_absolute_ev_breach`, `check_all` | 完整 |
| `E8E9E10EliminationChecker` | `__init__`, `check_e8_tail_risk`, `check_e9_minsky`, `check_e10_state_dependency`, `check_all` | 完整 |
| `E7UnexplainedReturnChecker` | `__init__`, `check` | 完整 |
| `E11QuantitativeSourceChecker` | `__init__`, `check` | 完整 |

### 方法完整性检查结果
- **空实现/占位符**：无。所有方法均有完整逻辑实现。
- **潜在问题**：`MultiStateSwitchBacktestScenario.run_scenario` 中 `strategy_factory(state)` 被调用但返回结果仅记录 `strategy_loaded`，未真正执行策略回测逻辑（仅做加载验证）。这是设计意图（状态机加载检查），但需注意其功能边界。

### 集成完备性
- **被谁调用**：
  - `risk_service.py:752` → `E7UnexplainedReturnChecker`
  - `strategy_ecosystem.py:235-250` → 全部7个检查器类（E7/E8/E9/E10/E11/E12/E13/WF6-10）
- **调用了谁**：仅依赖标准库（`logging`, `math`, `numpy`, `typing`, `dataclasses`, `collections`），无项目内部模块调用。
- **集成结论**：✅ 已被核心生产代码（RiskService、StrategyEcosystem）调用，集成完备。

### 方法重复/冲突
- 与其他模块无重复方法名。
- `check_all` 在 `WF6ToWF10EliminationChecker` 和 `E8E9E10EliminationChecker` 中方法名相同但属于不同类，无冲突。

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 12 | `0.3` | `max_correlation_threshold` 默认值 | ✅ 已在 `__init__` 参数化 |
| 13 | `20` | `min_trade_count` 默认值 | ✅ 已在 `__init__` 参数化 |
| 42 | `0.20` | `min_param_diff_pct` 默认值 | ✅ 已在 `__init__` 参数化 |
| 43 | `0.7` | `max_signal_sync_rate` 默认值 | ✅ 已在 `__init__` 参数化 |
| 84 | `10` | `min_hold_bars` 默认值 | ✅ 已在 `__init__` 参数化 |
| 86-87 | 状态字符串序列 | 硬编码默认状态序列 | ⚠️ 建议参数化，但属于测试场景配置，可保留 |
| 124 | `3` | WF6 最少窗口数 | 影响策略逻辑的阈值，建议参数化为类常量或构造参数 |
| 128 | `0.0` | sharpe 默认值 | 索引/默认值，可保留 |
| 134 | `2` | 连续下降次数阈值 | **策略逻辑关键参数**，建议参数化 |
| 139 | `2` | WF7 最少窗口数 | 建议参数化 |
| 144 | `0.5` | fragility 阈值 | **策略逻辑关键参数**，建议参数化 |
| 150 | `20.0` | `threshold_pct` 默认值 | ✅ 已在方法签名参数化 |
| 151 | `3` | WF9 最少窗口数 | 建议参数化 |
| 156 | `100` | 百分比换算 | 数学常量，可保留 |
| 160 | `2` | 连续下降次数阈值 | 建议参数化 |
| 180 | `0.05` | `tail_risk_threshold` 默认值 | ✅ 已参数化 |
| 181 | `0.3` | `minsky_threshold` 默认值 | ✅ 已参数化 |
| 182 | `0.8` | `state_dependency_threshold` 默认值 | ✅ 已参数化 |
| 192 | `20` | 尾部 5% 分位数 | `n // 20`，数学定义，可保留 |
| 257 | `15.0` | `residual_threshold_pct` 默认值 | ✅ 已参数化 |
| 285-286 | 允许来源列表 | 硬编码字符串列表 | 配置数据，建议类常量或配置文件 |

### 该模块排查结论
- **状态**：✅ 生产就绪
- **风险点**：WF6/WF7/WF9 中的最少窗口数和连续下降阈值等硬编码数字影响淘汰逻辑，建议提取为类常量或构造参数。
- **建议**：将 `3`、`2`、`0.5` 等阈值提升为构造参数，便于 walk-forward 调参。

---

## 2. order_persistence.py

### 模块总览
- **文件路径**：`ali2026v3_trading/order_persistence.py`
- **行数**：160
- **类数**：3
- **方法数**：约 14 个

### 类与方法清单

| 类名 | 方法 | 状态 |
|------|------|------|
| `OrderRecord` (dataclass) | — | 完整 |
| `SelfTradeDetector` | `__init__`, `add_order`, `check_self_trade`, `remove_order`, `get_self_trade_history` | 完整 |
| `NetworkRetryManager` | `__init__`, `get_retry_interval`, `should_retry`, `record_retry`, `reset_retry`, `execute_with_retry` | 完整 |
| `PartialFillHandler` | `__init__`, `record_partial_fill`, `check_and_cancel_remaining`, `get_pending_partial_fills` | 完整 |

### 方法完整性检查结果
- **空实现/占位符**：无。
- **潜在问题**：
  - `PartialFillHandler.check_and_cancel_remaining` 的 `cancel_func` 为可选参数，若传入 `None` 则仅标记状态并记录日志，不真正撤单。这是设计意图，但调用方需确保传入实际撤单函数。
  - `NetworkRetryManager.execute_with_retry` 在 `should_retry` 为 `False` 时返回 `None`，调用方需区分"成功返回None"和"无需重试"。

### 集成完备性
- **被谁调用**：
  - `position_service.py:195` → `SelfTradeDetector`, `NetworkRetryManager`, `PartialFillHandler`
  - `position_service.py:572` → `OrderRecord`
- **调用了谁**：仅标准库（`logging`, `json`, `time`, `typing`, `dataclasses`, `collections`）。
- **集成结论**：✅ 已被 PositionService 集成调用，用于自成交检测、网络重试、部分成交处理。

### 方法重复/冲突
- 无与其他模块重复的方法名。
- `OrderRecord` 为简单 dataclass，与 `order_service.py` 中可能存在的订单类无直接冲突。

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 66 | `3` | `max_retries` 默认值 | ✅ 已参数化 |
| 67 | `2.0` | `base_interval_sec` 默认值 | ✅ 已参数化 |
| 74 | `2` | 指数退底乘数 `2 ** count` | 算法常量，可保留 |
| 107 | `300.0` | `timeout_sec` 默认值 | ✅ 已参数化 |

### 该模块排查结论
- **状态**：✅ 生产就绪
- **风险点**：`execute_with_retry` 在最大重试次数用完后会抛出异常，调用方需有异常处理；`check_and_cancel_remaining` 的 `cancel_func` 为可选，需确保生产环境传入真实撤单函数。
- **建议**：无紧急修改需求。

---

## 3. health_check_api.py

### 模块总览
- **文件路径**：`ali2026v3_trading/health_check_api.py`
- **行数**：135
- **类数**：2
- **方法数**：约 9 个

### 类与方法清单

| 类名 | 方法 | 状态 |
|------|------|------|
| `StructuredJsonlLogger` | `__init__`, `log_signal`, `log_order`, `log_close` | 完整 |
| `HealthCheckAPI` | `__init__`, `update_component_status`, `update_open_position_count`, `update_last_signal_time`, `update_last_order_time`, `get_health_status`, `check_l1_alerts` | 完整 |

### 方法完整性检查结果
- **空实现/占位符**：无。
- **潜在问题**：
  - `HealthCheckAPI` 的 `_component_status` 硬编码了 6 个组件名称，若系统新增组件需同步修改此处。
  - `check_l1_alerts` 目前仅检查 `daily_drawdown` 和 `max_open_positions`，未覆盖 `l1_circuit_breaker`（虽然配置中有该字段）。

### 集成完备性
- **被谁调用**：
  - `position_service.py:210` → `StructuredJsonlLogger`
  - `signal_service.py:104` → `StructuredJsonlLogger`
  - `ProductionQuantSystem.py:39` → `HealthCheckAPI`, `StructuredJsonlLogger`
- **调用了谁**：仅标准库（`logging`, `json`, `time`, `os`, `typing`, `dataclasses`, `datetime`）。
- **集成结论**：✅ 已被 PositionService、SignalService、ProductionQuantSystem 调用，集成完备。

### 方法重复/冲突
- 无重复方法名。
- `log_signal` / `log_order` / `log_close` 为日志专用，与其他模块的日志方法无冲突。

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 81 | `0.05` | `daily_drawdown` 阈值 | **策略风控参数**，建议参数化或从配置读取 |
| 83 | `10` | `max_open_positions` 阈值 | **策略风控参数**，建议参数化或从配置读取 |
| 103 | `300` | signal 超时秒数（5分钟） | 建议参数化 |
| 107 | `600` | order 超时秒数（10分钟） | 建议参数化 |

### 该模块排查结论
- **状态**：⚠️ 基本可用，但有改进空间
- **风险点**：
  1. `l1_circuit_breaker` 配置项在 `check_l1_alerts` 中未被实际使用。
  2. `300` 和 `600` 秒的超时阈值硬编码，建议提取为构造参数。
- **建议**：
  - 将 `daily_drawdown`、`max_open_positions`、`signal_stale_sec`、`order_stale_sec` 提取为 `__init__` 参数。
  - 在 `check_l1_alerts` 中补充 `l1_circuit_breaker` 的检查逻辑（或从配置中移除该字段）。

---

## 4. data_service.py

### 模块总览
- **文件路径**：`ali2026v3_trading/data_service.py`
- **行数**：284
- **类数**：1（`DataService`）
- **方法数**：约 8 个（类方法）+ 模块级便捷函数 12 个

### 类与方法清单

| 类/函数 | 方法 | 状态 |
|---------|------|------|
| `DataService` | `__init__`, `_initialize`, `params_service` (property), `mark_products_loaded`, `_preload_realtime_cache` | 完整 |
| 模块级 | `get_data_service`, `query`, `get_latest_price`, `get_time_range`, `get_daily_aggregates`, `get_symbol_daily_ohlc`, `batch_get_latest_prices`, `explain`, `refresh_data`, `batch_insert_ticks`, `incremental_load`, `clear_cache` | 完整 |

### 方法完整性检查结果
- **空实现/占位符**：无。
- **潜在问题**：
  - `DataService` 是 Facade 类，实际功能由 5 个 Mixin（`DBConnectionMixin`, `QueryCacheMixin`, `OptionSyncMixin`, `DataWriterMixin`, `SchemaManagerMixin`）提供。本文件仅做组合和初始化，需确保各 Mixin 文件存在且功能完整（本次排查范围外，但需注意）。
  - `_preload_realtime_cache` 中 `bid_price=0.0, ask_price=0.0` 为占位值，不影响核心功能。
  - `DB_FILE` 和 `PARQUET_PATH` 被定义为 `property(lambda self: ...)`，但模块级属性绑定到类上，实际使用中通过 `self.DB_FILE` 访问，逻辑正确。

### 集成完备性
- **被谁调用**：广泛被调用（96个文件匹配），核心调用方包括：
  - `position_service.py`, `risk_service.py`, `strategy_tick_handler.py`, `order_service.py`, `box_spring_strategy.py`, `t_type_service.py`, `params_service.py`, `strategy_core_service.py`, `subscription_manager.py` 等。
- **调用了谁**：
  - 子模块：`ds_realtime_cache`, `ds_db_connection`, `ds_query_cache`, `ds_option_sync`, `ds_data_writer`, `ds_schema_manager`
  - 惰性导入：`params_service.get_params_service`
- **集成结论**：✅ 核心基础设施，被大量模块调用，集成完备。

### 方法重复/冲突
- `get_data_service` 为单例工厂，与 `params_service.get_params_service`、`t_type_service.get_t_type_service` 等模式一致，无冲突。
- `query` 模块级函数与 `QueryCacheMixin.query` 方法名相同，但模块级函数是代理调用，无冲突。

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 65 | `0.75` | DuckDB 内存占用比例 | **系统配置参数**，建议环境变量化（已有部分环境变量支持） |
| 66 | `1024**3` | GB 换算 | 数学常量，可保留 |
| 67 | `4` | CPU 数量回退值 | 系统配置，可保留 |
| 70 | `128` | `QUERY_CACHE_SIZE` 默认值 | 建议环境变量化（已有） |
| 71 | `60` | `QUERY_CACHE_TTL` 默认值 | 建议环境变量化（已有） |
| 137 | `100` | `max_recent_ticks` | RealTimeCache 参数，建议参数化 |

### 该模块排查结论
- **状态**：✅ 生产就绪（Facade 模式，依赖子 Mixin 实现）
- **风险点**：
  1. 实际功能分散在 5 个子模块中，需确保各子模块在生产环境中无异常。
  2. `DUCKDB_MAX_MEMORY` 的计算逻辑 `int(_total_mem * 0.75 / (1024**3))` 在内存极大或极小的机器上可能不够精确。
- **建议**：将 `0.75` 提取为环境变量默认值（如 `DUCKDB_MEMORY_RATIO`）。

---

## 5. box_detector.py

### 模块总览
- **文件路径**：`ali2026v3_trading/box_detector.py`
- **行数**：606
- **类数**：4（`BoxProfile`, `ExtremeState`, `BoxStrategyParams`, `BoxDetector`）
- **方法数**：约 18 个

### 类与方法清单

| 类名 | 方法 | 状态 |
|------|------|------|
| `BoxProfile` (dataclass) | `is_valid` (property), `to_dict` | 完整 |
| `ExtremeState` (dataclass) | `to_dict` | 完整 |
| `BoxStrategyParams` (dataclass) | `to_dict` | 完整 |
| `BoxDetector` | `__init__`, `params` (property), `update_bar`, `update_iv`, `_compute_adx_simplified`, `_find_support_resistance`, `detect_box`, `get_current_box`, `classify_extreme_state`, `check_iv_filter`, `_compute_iv_percentile`, `check_order_flow_exhaustion`, `determine_trade_direction`, `get_extreme_state`, `get_health_status`, `get_stats`, `get_box_history`, `estimate_potential_plr` | 完整 |

### 方法完整性检查结果
- **空实现/占位符**：无。
- **潜在问题**：
  - `_compute_adx_simplified` 是简化版 ADX，未使用平滑移动平均（Wilder's smoothing），与标准 ADX 有差异。需确认策略逻辑是否接受此简化。
  - `detect_box` 中使用了未定义的变量 `box_height`（第367行），存在 **NameError 风险**：
    ```python
    if box_height > 1e-10:   # ❌ box_height 未定义
    ```
    应为 `box_range = box_upper - box_lower` 或类似变量。
  - `classify_extreme_state` 中 `confidence` 计算公式（第463-468行）硬编码了权重 `0.25, 0.30, 0.25, 0.20`，与类常量 `PRICE_SCORE_WEIGHT` 等不一致（类常量定义了但未使用）。

### 集成完备性
- **被谁调用**：
  - `strategy_ecosystem.py:42` → `BoxDetector`, `BoxProfile`, `ExtremeState`, `BoxStrategyParams`
  - `box_detector.py` 自身通过 `get_box_detector` 工厂函数提供单例
  - 测试文件：`tests/test_box_order.py`, `tests/test_strategy_ecosystem.py`
- **调用了谁**：仅标准库（`logging`, `math`, `threading`, `time`, `bisect`, `collections`, `dataclasses`, `datetime`, `typing`）。
- **集成结论**：✅ 已被 StrategyEcosystem 集成调用。

### 方法重复/冲突
- `update_iv` 与 `box_spring_strategy.py` 中的 `update_iv` 方法名相同但属于不同类，无直接冲突。
- `get_health_status` 与 `BoxSpringStrategy.get_health_status` 方法名相同，但返回结构不同，调用方需区分。

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 97 | `30` | `max_hold_minutes` 默认值 | ✅ 已在 dataclass 参数化 |
| 98 | `0.4` | `take_profit_ratio` 默认值 | ✅ 已参数化 |
| 99 | `0.3` | `stop_loss_ratio` 默认值 | ✅ 已参数化 |
| 100 | `0.05` | `max_risk_ratio` 默认值 | ✅ 已参数化 |
| 101 | `50.0` | `iv_percentile_min` 默认值 | ✅ 已参数化 |
| 102 | `120.0` | `signal_cooldown_sec` 默认值 | ✅ 已参数化 |
| 103 | `0.3` | `position_scale` 默认值 | ✅ 已参数化 |
| 104 | `1` | `lots_min` 默认值 | ✅ 已参数化 |
| 105 | `10` | `option_buy_lots_max` 默认值 | ✅ 已参数化 |
| 106 | `0.6` | `min_extreme_confidence` 默认值 | ✅ 已参数化 |
| 107 | `2` | `min_bounce_count` 默认值 | ✅ 已参数化 |
| 108 | `5.0` | `box_width_max_pct` 默认值 | ✅ 已参数化 |
| 125 | `50.0` | `ADX_DEFAULT_VALUE` | 建议参数化（类常量已定义，但不可配置） |
| 126 | `100.0` | `ADX_MULTIPLIER` | 数学常量，可保留 |
| 127 | `100` | `BOX_HISTORY_MAXLEN` | 建议参数化 |
| 128 | `0.2` | `FLOW_IMBALANCE_THRESHOLD` | **策略逻辑参数**，建议构造参数化 |
| 129 | `0.01` | `CVD_SLOPE_THRESHOLD` | **策略逻辑参数**，建议构造参数化 |
| 130-136 | 各权重常量 | 评分权重 | **策略逻辑参数**，建议构造参数化 |
| 137 | `0.15` | `BOTTOM_THRESHOLD_RATIO` | **策略逻辑参数**，建议构造参数化 |
| 138 | `0.15` | `TOP_THRESHOLD_RATIO` | **策略逻辑参数**，建议构造参数化 |
| 143 | `60` | `lookback_bars` 默认值 | ✅ 已参数化 |
| 144 | `20` | `min_box_bars` 默认值 | ✅ 已参数化 |
| 145 | `14` | `adx_period` 默认值 | ✅ 已参数化 |
| 146 | `25.0` | `adx_threshold` 默认值 | ✅ 已参数化 |
| 147 | `0.1` | `bounce_tolerance_pct` 默认值 | ✅ 已参数化 |
| 148 | `1000` | `iv_history_maxlen` 默认值 | ✅ 已参数化 |
| 224 | `period + 1` | ADX 计算最小长度 | 算法依赖，可保留 |
| 290 | `0.3` | `_find_support_resistance` tolerance_pct | ✅ 已参数化 |
| 314 | `6` | `box_id` 零填充宽度 | 格式化常量，可保留 |
| 362-364 | `1.0`, `2` | 评分计算中的常数 | 数学/算法常数，可保留 |
| 372 | `3.0` | PLR 评分归一化分母 | 建议参数化 |
| 427 | `100.0` | 百分比换算 | 数学常量，可保留 |
| 458 | `50.0`, `100.0` | price_score 计算 | 数学常量，可保留 |
| 460 | `100.0` | iv_score 归一化 | 数学常量，可保留 |

### 该模块排查结论
- **状态**：❌ **存在代码缺陷，需立即修复**
- **关键缺陷**：
  1. **第367行 `box_height` 未定义**，会导致 `NameError`。
  2. `classify_extreme_state` 中硬编码了权重（`0.25, 0.30, 0.25, 0.20`），与类常量 `WIDTH_SCORE_WEIGHT` 等不一致，类常量未实际使用。
- **建议**：
  1. 立即修复 `box_height` 为 `box_range = box_upper - box_lower`。
  2. 将 `classify_extreme_state` 中的硬编码权重替换为类常量，或将类常量提升为构造参数。
  3. 将 `FLOW_IMBALANCE_THRESHOLD`、`CVD_SLOPE_THRESHOLD`、`BOTTOM_THRESHOLD_RATIO`、`TOP_THRESHOLD_RATIO` 等策略关键参数提升为 `BoxStrategyParams` 或构造参数。

---

## 6. box_spring_strategy.py

### 模块总览
- **文件路径**：`ali2026v3_trading/box_spring_strategy.py`
- **行数**：1378
- **类数**：5（`SpringState`, `BoxRange`, `SpringSignal`, `SpringPosition`, `PendingPullback`, `BoxSpringStrategy`）
- **方法数**：约 32 个

### 类与方法清单

| 类名 | 方法 | 状态 |
|------|------|------|
| `SpringState` (Enum) | — | 完整 |
| `BoxRange` | `contains_price`, `price_position` | 完整 |
| `SpringSignal` (dataclass) | — | 完整 |
| `SpringPosition` | `pnl_ratio` (property), `should_take_profit` (property), `should_accept_loss` (property), `adjust_tp_sl_by_plr` | 完整 |
| `PendingPullback` | `check_retrace` | 完整 |
| `BoxSpringStrategy` | `__init__`, `set_capital_scale`, `get_capital_scale`, `update_box`, `get_active_box`, `_invalidate_box`, `update_iv`, `get_iv_percentile`, `detect_spring`, `_infer_direction`, `_estimate_gamma_exposure`, `estimate_plr_before_entry`, `check_trigger`, `check_pullback_entry`, `_process_pending_pullbacks`, `get_pullback_stats`, `execute_spring_entry`, `_execute_straddle_entry`, `_find_straddle_pair`, `on_premium_update`, `_evaluate_close_straddle`, `_evaluate_close`, `_execute_close`, `is_spring_position`, `prevent_trend_conversion`, `get_expected_value`, `_check_cross_strategy_risk`, `_record_spring_trade`, `get_stats`, `get_health_status`, `on_tick`, `_get_greeks_calculator`, `scan_springs`, `_scan_from_sort_buckets`, `_scan_from_option_info`, `_evaluate_candidate` | 完整 |

### 方法完整性检查结果
- **空实现/占位符**：无。
- **潜在问题**：
  1. `_process_pending_pullbacks` 方法返回值始终为 `[]`（第537行），`ready` 列表被计算但未返回。疑似 **逻辑缺陷**。
  2. `_execute_straddle_entry` 中使用了未在当前作用域定义的 `estimated_plr` 变量（第704行），存在 **NameError 风险**（该变量在 `execute_spring_entry` 中定义，但 `_execute_straddle_entry` 独立调用时未定义）。
  3. `_find_straddle_pair` 大量依赖 `t_type._width_cache` 的内部属性（`_lock`, `_option_info`, `_future_rising`），存在 **内部 API 变更导致的脆弱性**。
  4. `on_tick` 中每 tick 都尝试导入 `GreeksCalculator` 和 `get_data_service`，存在 **性能开销**，建议提升为实例属性或惰性初始化。
  5. `_evaluate_candidate` 中硬编码 `days_to_expiry = 3`（第1350行），当无法从缓存获取到期日时 fallback 为固定值，可能影响策略逻辑。

### 集成完备性
- **被谁调用**：
  - `mode_engine.py:771, 816` → `get_box_spring_strategy`
  - `strategy_ecosystem.py:331` → `get_box_spring_strategy`
  - 测试文件：`tests/test_spring_order.py`
- **调用了谁**：
  - `order_service.get_order_service`
  - `data_service.get_data_service`
  - `t_type_service.get_t_type_service`
  - `risk_service.get_risk_service`
  - `position_service.get_position_service`, `aggregate_greeks_exposure`, `get_cross_strategy_risk_guard`
  - `strategy_ecosystem.get_strategy_ecosystem`
  - `greeks_calculator.GreeksCalculator`
  - `shared_utils.PullbackManager`
- **集成结论**：✅ 已被 ModeEngine、StrategyEcosystem 调用，且依赖大量核心服务，集成深度高。

### 方法重复/冲突
- `update_iv` 与 `BoxDetector.update_iv` 方法名相同，但属于不同类，无直接冲突。
- `get_health_status` 与 `BoxDetector.get_health_status` 方法名相同，返回结构不同，需注意调用方区分。
- `get_stats` 与 `BoxDetector.get_stats` 方法名相同，返回结构不同。

### 魔法数字清单

| 行号 | 数字 | 上下文 | 建议 |
|------|------|--------|------|
| 183 | `120` | `iv_lookback_bars` 默认值 | ✅ 已从 params 读取 |
| 188 | `3` | `min_box_touches` 默认值 | ✅ 已从 params 读取 |
| 189 | `0.04` | `max_box_width_pct` 默认值 | ✅ 已从 params 读取 |
| 190 | `5.0` | `iv_low_percentile` 默认值 | ✅ 已从 params 读取 |
| 191 | `2.0` | `iv_very_low_percentile` 默认值 | ✅ 已从 params 读取 |
| 192 | `2` | `min_days_to_expiry` 默认值 | ✅ 已从 params 读取 |
| 193 | `5` | `max_days_to_expiry` 默认值 | ✅ 已从 params 读取 |
| 194 | `0.015` | `max_premium_cost_pct` 默认值 | ✅ 已从 params 读取 |
| 195 | `5.0` | `stop_profit_ratio` 默认值 | ✅ 已从 params 读取 |
| 196 | `0.95` | `max_loss_pct` 默认值 | ✅ 已从 params 读取 |
| 197 | `0.015` | `max_position_pct` 默认值 | ✅ 已从 params 读取 |
| 199 | `0.0` | `min_estimated_plr` 默认值 | ✅ 已从 params 读取 |
| 201 | `300` | `spring_cooldown_sec` 默认值 | ✅ 已从 params 读取 |
| 202 | `3` | `max_spring_positions` 默认值 | ✅ 已从 params 读取 |
| 205 | `5` | `pullback_wait_bars` 默认值 | ✅ 已从 params 读取 |
| 206 | `0.15` | `pullback_retrace_pct` 默认值 | ✅ 已从 params 读取 |
| 207 | `20.0` | `pullback_iv_min_percentile` 默认值 | ✅ 已从 params 读取 |
| 208 | `80.0` | `pullback_iv_max_percentile` 默认值 | ✅ 已从 params 读取 |
| 265 | `1.0` | width_pct 计算回退 | 数学常量，可保留 |
| 273 | `1.005`, `0.995` | 箱体突破阈值 | **策略逻辑参数**，建议参数化 |
| 277 | `0.998`, `1.002` | 箱体触碰阈值 | **策略逻辑参数**，建议参数化 |
| 317 | `50.0` | IV 历史不足时回退百分位 | 建议参数化 |
| 325 | `5` | IV 历史最小长度 | 建议参数化 |
| 371 | `0.3`, `0.7` | 价格位置过滤阈值 | **策略逻辑参数**，建议参数化 |
| 375 | `0.02` | 行权价距离阈值 | **策略逻辑参数**，建议参数化 |
| 384 | `cooldown_sec` | 已参数化 | ✅ |
| 422-427 | `0.45`, `0.55` | 方向推断阈值 | **策略逻辑参数**，建议参数化 |
| 436 | `0.5`, `365.0` | Gamma 估算中的常数 | 数学/金融公式常数，可保留 |
| 483 | `0.3` | `order_flow_imbalance` 触发阈值 | **策略逻辑参数**，建议参数化 |
| 487 | `1.5` | `option_chain_activity` 触发阈值 | **策略逻辑参数**，建议参数化 |
| 574-578 | `BUY`, `OPEN` | 方向/动作映射 | 枚举常量，可保留 |
| 647, 660, 673, 684, 885, 893 | `1` | 下单手数 | 建议参数化（`signal.lots` 已存在，但 straddle 中硬编码为1） |
| 850, 866 | `120` | `max_spring_hold_minutes` 默认值 | 已从 params 读取，但 fallback 硬编码 |
| 869 | `20` | EV 降级检查最小交易数 | 建议参数化 |
| 1071 | `20` | `max_active_positions` 警告阈值 | 建议参数化 |
| 1245 | `0.03` | 扫描时行权价距离过滤 | **策略逻辑参数**，建议参数化 |
| 1249 | `999` | 默认到期天数 | fallback 值，建议参数化 |
| 1272, 1189 | `3`, `5` | 扫描候选数量 | 建议参数化 |
| 1350 | `3` | `days_to_expiry` fallback | 建议参数化 |

### 该模块排查结论
- **状态**：❌ **存在代码缺陷，需立即修复**
- **关键缺陷**：
  1. **第537行 `_process_pending_pullbacks` 始终返回 `[]`**，`ready` 信号被丢弃，回撤开仓逻辑失效。
  2. **第704行 `_execute_straddle_entry` 中 `estimated_plr` 未定义**，会导致 `NameError`。
  3. **第273、277行** 箱体突破/触碰阈值硬编码，建议参数化。
  4. `on_tick` 中高频导入导致性能开销。
- **建议**：
  1. 立即修复 `_process_pending_pullbacks` 返回 `ready` 而非 `[]`。
  2. 立即修复 `_execute_straddle_entry` 中 `estimated_plr` 的计算或传入。
  3. 将箱体突破阈值（`1.005/0.995`）、触碰阈值（`0.998/1.002`）、触发阈值（`0.3`, `1.5`）等提取为 `params` 配置项。
  4. 将 `on_tick` 中的导入提升为实例级别的惰性初始化属性。

---

## 综合结论与优先级

### 按模块总结

| 模块 | 状态 | 关键缺陷数 | 魔法数字需参数化数 |
|------|------|-----------|------------------|
| governance_engine.py | ✅ 生产就绪 | 0 | 3（WF阈值） |
| order_persistence.py | ✅ 生产就绪 | 0 | 0 |
| health_check_api.py | ⚠️ 基本可用 | 0 | 4（超时/阈值） |
| data_service.py | ✅ 生产就绪 | 0 | 1（内存比例） |
| box_detector.py | ❌ 需修复 | 2（NameError+权重不一致） | 8（策略阈值） |
| box_spring_strategy.py | ❌ 需修复 | 2（返回空+NameError） | 12（策略阈值） |

### 最高优先级修复项

1. **[P0] `box_detector.py:367` `box_height` 未定义** → NameError
2. **[P0] `box_spring_strategy.py:537` `_process_pending_pullbacks` 始终返回 `[]`** → 功能失效
3. **[P0] `box_spring_strategy.py:704` `_execute_straddle_entry` 中 `estimated_plr` 未定义** → NameError
4. **[P1] `box_detector.py` `classify_extreme_state` 硬编码权重与类常量不一致** → 维护风险
5. **[P1] 多个模块中策略关键阈值硬编码** → 建议统一参数化

### 跨模块重复/冲突汇总

- `get_health_status`：`BoxDetector` 与 `BoxSpringStrategy` 方法名重复，返回结构不同，调用方需通过实例类型区分。
- `get_stats`：`BoxDetector` 与 `BoxSpringStrategy` 方法名重复，返回结构不同。
- `update_iv`：`BoxDetector` 与 `BoxSpringStrategy` 方法名重复，逻辑不同（全局 vs 按品种）。

以上重复属于不同类的同名方法，Python 中无冲突，但开发者在阅读代码时需注意上下文。
