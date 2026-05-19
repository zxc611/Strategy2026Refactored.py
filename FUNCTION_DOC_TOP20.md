# ali2026v3_trading 全部59核心模块 功能文档

> 生成时间: 2026-05-15  
> 审查范围: 按字母排序全部59个核心模块（第1批1-20 + 第2批21-40 + 第3批41-59）

---

## 模块1: `__init__.py` (128行)

### 职责
量化交易系统T型图架构包入口，提供延迟导入(lazy import)机制和InstrumentDataManager单例工厂。

### 类
| 类名 | 描述 |
|------|------|
| `InstrumentDataManager` | 期货/期权数据管理器，Mixin组合类（`_StorageCoreMixin` + `_StorageQueryMixin`） |

### 函数/方法
| 名称 | 签名 | 描述 |
|------|------|------|
| `__getattr__(name)` | `(name: str) -> Any` | 延迟导入机制，从`_EXPORTS`表按需加载模块属性 |
| `__dir__()` | `() -> List[str]` | 返回模块公开属性列表 |
| `get_instrument_data_manager()` | `() -> InstrumentDataManager` | 全局单例工厂（双重检查锁），首次调用时创建InstrumentDataManager实例 |

### 导出常量
| 名称 | 值 |
|------|-----|
| `__version__` | `'1.2.0'` |
| `__all__` | 17个lazy import名称列表 |

---

## 模块2: `box_detector.py` (547行)

### 职责
箱体检测与极值判断引擎，识别价格箱体结构并在箱体边界判断极值子状态。

### 数据类
| 类名 | 描述 |
|------|------|
| `BoxProfile` | 箱体轮廓（box_id, is_box, upper, lower, median, width_pct, confidence, adx等） |
| `ExtremeState` | 极值子状态（extreme_type, is_bottom_extreme, is_top_extreme, tradeable等） |
| `BoxStrategyParams` | 箱体策略参数（止盈/止损比例, IV过滤, 信号冷却期等） |

### 核心类: `BoxDetector`
| 方法 | 签名 | 描述 |
|------|------|------|
| `__init__` | `(params, lookback_bars=60, min_box_bars=20, adx_period=14, ...)` | 初始化，设置价格序列deque、IV历史、统计计数器 |
| `update_bar` | `(high, low, close, volume, timestamp)` | 输入K线数据，追加到价格序列 |
| `update_iv` | `(iv: float)` | 输入IV值，维护排序IV序列用于百分位计算 |
| `detect_box` | `() -> BoxProfile` | 核心方法：基于价格振幅+ADX+支撑阻力聚类识别箱体 |
| `get_current_box` | `() -> Optional[BoxProfile]` | 获取当前活跃箱体 |
| `classify_extreme_state` | `(current_price, resonance_direction, resonance_strength, current_iv, flow_imbalance, cvd_slope) -> ExtremeState` | 判断极值子状态（箱底/箱顶），综合共振+IV+订单流 |
| `check_iv_filter` | `(current_iv: float) -> bool` | IV高位过滤（百分位≥阈值） |
| `check_order_flow_exhaustion` | `(flow_imbalance, cvd_slope) -> bool` | 订单流衰竭确认 |
| `determine_trade_direction` | `(extreme_state) -> str` | 极值→交易方向（bottom→long, top→short） |
| `get_health_status` | `() -> Dict` | 健康状态快照 |
| `get_stats` | `() -> Dict` | 统计信息 |
| `get_box_history` | `(limit=20) -> List[Dict]` | 箱体历史 |

### 静态方法
| 方法 | 描述 |
|------|------|
| `_compute_adx_simplified(highs, lows, closes, period)` | 简化版ADX计算 |
| `_find_support_resistance(lows, highs, n_clusters, tolerance_pct)` | 支撑/阻力位聚类识别 |

### 模块级函数
| 函数 | 描述 |
|------|------|
| `get_box_detector(**kwargs)` | 全局单例工厂 |

---

## 模块3: `box_spring_strategy.py` (1183行)

### 职责
箱体波动率脉冲策略（弹簧策略），交易波动率从极低回归正常的脉冲，而非价格方向。

### 数据类/枚举
| 类名 | 描述 |
|------|------|
| `SpringState(Enum)` | 弹簧状态：DORMANT/COMPRESSED/TRIGGERED/ACTIVE/EXPIRED |
| `BoxRange` | 箱体范围（box_top, box_bottom, touch_count, is_active） |
| `SpringSignal` | 弹簧信号（spring_state, iv_percentile, direction, strike_price等） |
| `SpringPosition` | 弹簧持仓（entry_premium, current_premium, stop_profit_ratio, is_open等） |

### 核心类: `BoxSpringStrategy`
| 方法 | 描述 |
|------|------|
| `update_box(instrument_id, high, low, close, timestamp)` | 更新/创建箱体 |
| `get_active_box(instrument_id)` | 获取活跃箱体（touch_count≥min） |
| `update_iv(instrument_id, iv)` | 更新IV并返回百分位 |
| `get_iv_percentile(instrument_id)` | 获取当前IV百分位 |
| `detect_spring(instrument_id, future_price, option_instrument_id, strike_price, iv, premium_price, days_to_expiry, account_equity)` | 弹簧识别：四条件同时满足 |
| `check_trigger(instrument_id, order_flow_imbalance, option_chain_activity)` | 入场触发：订单流异动/期权链活跃度 |
| `execute_spring_entry(signal)` | 下单执行（含跨策略风控检查） |
| `on_premium_update(option_instrument_id, current_premium)` | 权利金更新→平仓评估 |
| `is_spring_position(instrument_id)` | 铁律检查：是否弹簧持仓 |
| `prevent_trend_conversion(instrument_id, proposed_action, proposed_reason)` | 铁律：阻止弹簧仓位转为趋势策略 |
| `get_expected_value()` | 期望值计算（胜率×盈亏比） |
| `scan_springs()` | 扫描全量期权寻找弹簧机会 |
| `on_tick(instrument_id, price, high, low, volume, timestamp)` | Tick驱动入口 |
| `get_stats()` / `get_health_status()` | 统计/健康状态 |

### 私有方法
| 方法 | 描述 |
|------|------|
| `_invalidate_box(instrument_id, reason)` | 失效箱体 |
| `_infer_direction(box, price, price_pos)` | 推断方向（BUY_CALL/BUY_PUT/BUY_STRADDLE） |
| `_estimate_gamma_exposure(S, K, sigma, T_days, premium)` | Gamma暴露估算 |
| `_execute_straddle_entry(signal)` | 跨式期权下单 |
| `_find_straddle_pair(signal)` | 寻找跨式配对 |
| `_evaluate_close(pos)` / `_evaluate_close_straddle(pos)` | 平仓评估 |
| `_execute_close(pos, reason)` | 执行平仓 |
| `_check_cross_strategy_risk(signal)` | 跨策略风控检查 |
| `_scan_from_sort_buckets()` / `_scan_from_option_info()` / `_evaluate_candidate()` | 扫描辅助方法 |

### 模块级函数
| 函数 | 描述 |
|------|------|
| `get_box_spring_strategy(params)` | 全局单例工厂 |

---

## 模块4: `config_exchange.py` (213行)

### 职责
交易所与合约配置模块，管理交易所配置、品种映射、合约解析工具。

### 数据类
| 类名 | 描述 |
|------|------|
| `ExchangeConfig` | 交易所配置（exchanges列表, simulated_instruments, option_products映射, product_exchanges映射, futures_switches开关） |

### ExchangeConfig方法
| 方法 | 描述 |
|------|------|
| `get_instruments_for_exchange(exchange)` | 获取交易所模拟合约 |
| `get_all_simulated_instruments()` | 所有模拟合约 |
| `set_future_switch(product_code, enabled)` | 设置品种开关 |
| `get_enabled_futures()` / `get_disabled_futures()` | 启用/禁用品种列表 |
| `is_future_enabled(product_code)` | 品种是否启用 |
| `should_generate_option(option_product, underlying)` | 是否生成期权 |
| `get_enabled_options()` / `get_disabled_options()` | 启用/禁用期权列表 |
| `get_generation_config()` | 生成配置汇总 |

### 模块级函数
| 函数 | 描述 |
|------|------|
| `_get_option_underlying_product(option_product)` | 期权→标的品种 |
| `ensure_products_with_retry(data_service, max_retries)` | 确保品种配置（带重试） |
| `build_exchange_mapping(custom_mapping)` | 构建交易所映射 |
| `resolve_product_exchange(product_or_instrument, exchange_mapping, default_exchange)` | 解析品种→交易所 |
| `make_platform_future_id(product, year_month)` | 构造期货合约ID |

---

## 模块5: `config_params.py` (462行)

### 职责
参数表管理模块，默认参数表、参数缓存、参数加载/校验/环境覆盖。

### 核心常量
| 名称 | 描述 |
|------|------|
| `DEFAULT_PARAM_TABLE` | 199个默认参数的字典 |
| `CACHE_TTL` | 缓存TTL=300秒 |

### 函数
| 函数 | 描述 |
|------|------|
| `get_cached_params()` | 获取缓存参数表（TTL机制） |
| `reset_param_cache()` | 重置参数缓存 |
| `update_cached_params(updates, sync_default_table)` | 更新缓存参数 |
| `load_default_params_from_json(json_path)` | 从JSON加载参数 |
| `get_params_metadata(json_path)` | 获取参数元数据 |
| `validate_params(params, json_path)` | 参数校验（类型+边界+约束C1-C12） |
| `apply_environment(params, env)` | 应用环境覆盖 |
| `rebuild_default_param_table(env)` | 重建默认参数表 |
| `load_option_params_from_file()` | 加载期权参数文件 |
| `merge_option_params_to_default()` | 合并期权参数到默认表 |

---

## 模块6: `config_service.py` (650行)

### 职责
统一配置管理中心，CQRS架构Configuration层，组合路径/交易/输出/日志/数据库/性能配置。

### 配置类
| 类名 | 描述 |
|------|------|
| `PathConfig` | 路径配置（project_root, db_path, log_dir等） |
| `TradingConfig` | 交易参数（default_volume, max_position_limit, order_timeout等） |
| `OutputConfig` | 输出配置（mode, diagnostic_output, debug_output） |
| `LoggingConfig` | 日志配置（log_level, log_format, rotation等） |
| `DatabaseConfig` | 数据库配置（db_type, db_path, pool_size等） |
| `DataPathsConfig` | 数据路径配置（duckdb_file, parquet_path, flush_windows） |
| `PerformanceConfig` | 性能监控配置 |
| `DebugConfig` | 调试配置 |

### 核心类: `ConfigService` (单例)
| 方法 | 描述 |
|------|------|
| `reload()` | 热重载配置文件 |
| `need_reload()` | 检查是否需要重载 |
| `to_dict()` | 序列化为字典 |
| `save_to_file(config_path)` | 保存到文件 |

### 模块级函数
| 函数 | 描述 |
|------|------|
| `get_config()` | 获取ConfigService单例 |
| `setup_logging()` | 初始化日志系统 |
| `setup_paths()` / `get_paths()` | 路径配置 |
| `get_default_db_path()` / `get_project_root()` | 便捷函数 |
| `reload_config()` | 重载配置 |

---

## 模块7: `cycle_resonance_module.py` (644行)

### 职责
周期共振模块——四策略风险定价基础设施，基于HMM状态+多周期趋势+五态分布输出四变量。

### 数据类/枚举
| 类名 | 描述 |
|------|------|
| `Phase(Enum)` | 相位：蓄力/释放/衰竭/混沌 |
| `CRParams` | 113个周期共振参数（dataclass） |
| `CycleResonanceOutput` | 输出四变量（directional_bias, resonance_strength, phase, state_entropy） |
| `RiskSurfaceAdjustment` | 风险曲面调节（size_multiplier, stop_loss_multiplier, max_hold_seconds, allow_overnight） |

### 核心类: `CycleResonanceModule`
| 方法 | 描述 |
|------|------|
| `update(hmm_state, hmm_posterior, trend_scores, trend_directions, strength, imbalance)` | 核心更新→四变量输出 |
| `get_risk_surface(strategy, output)` | 根据策略类型计算风险曲面调节 |
| `check_portfolio_constraints(positions, output)` | 组合层面硬性约束检查 |
| `check_circuit_breaker(output, entropy_sustained_minutes, daily_drawdown_pct)` | 熔断检查 |
| `get_signal_priority(phase)` | 策略优先级排序 |
| `get_spring_threshold_adjustment(output)` | 弹簧策略非对称阈值 |
| `get_hft_floor_adjustment(resonance_signal_active, output)` | 高频仓位下限调整 |

### 私有方法
| 方法 | 描述 |
|------|------|
| `_compute_directional_bias(trend_scores, trend_directions, imbalance)` | 方向偏置计算 |
| `_compute_resonance_strength(trend_scores, strength, hmm_posterior)` | 共振强度计算 |
| `_compute_state_entropy(hmm_state)` | 状态熵计算 |
| `_compute_phase(resonance_strength, state_entropy, hmm_state, directional_bias)` | 相位判定 |
| `_high_freq_risk_surface(output, is_co_aligned, is_chaos)` | 高频策略风险曲面 |
| `_resonance_risk_surface(output, is_chaos, is_release)` | 共振策略风险曲面 |
| `_box_risk_surface(output, hmm_state)` | 箱形策略风险曲面 |
| `_spring_risk_surface(output, is_charge, is_release)` | 弹簧策略风险曲面 |

### 模块级函数
| 函数 | 描述 |
|------|------|
| `get_cycle_resonance_module(**kwargs)` | 全局单例工厂（sys级存储） |

---

## 模块8: `data_service.py` (266行)

### 职责
数据服务模块，组合5个Mixin提供统一数据访问接口。

### 核心类: `DataService`
| 方法 | 描述 |
|------|------|
| `_initialize()` | 初始化DB/缓存/监控 |
| `params_service` | 惰性获取ParamsService |
| `mark_products_loaded()` | 标记品种配置已加载 |
| `_preload_realtime_cache()` | 预加载最新价格 |

### 便捷函数
| 函数 | 描述 |
|------|------|
| `query(sql, params)` | 执行SQL查询 |
| `get_latest_price(symbol)` | 获取最新价格 |
| `get_time_range(instrument_id, start, end)` | 时间范围查询 |
| `get_daily_aggregates(start_date, end_date)` | 日聚合 |
| `get_symbol_daily_ohlc(symbol, start_date, end_date)` | 品种日OHLC |
| `batch_get_latest_prices(symbols)` | 批量获取最新价格 |
| `explain(sql)` / `refresh_data()` / `clear_cache()` | 其他便捷方法 |
| `batch_insert_ticks(ticks_data, use_arrow)` | 批量插入tick |
| `incremental_load(new_parquet_path)` | 增量加载 |

---

## 模块9: `delay_time_sharpe_3d.py` (948行)

### 职责
延迟-时间-夏普三维网格搜索系统，用于参数池网格回测和实时延迟自适应策略选择。

### 数据类
| 类名 | 描述 |
|------|------|
| `TimeParams` | 时间参数（strategy, period, bar_count, delay_ms） |

### 核心类: `DelayTimeSharpe3D`
| 方法 | 描述 |
|------|------|
| `__init__(db_path)` | 初始化DuckDB连接 |
| `_init_tables()` / `_seed_default_params()` | 建表和种子数据 |
| `generate_variants(default, tick_range, bar_range)` | 生成参数变体 |
| `run_3d_grid_search(bar_data, ...)` | 三维网格搜索 |
| `get_optimal_params(delay_ms, objective, strategy)` | 查询最优参数 |
| `get_sharpe_surface(strategy, delay_ms)` | 夏普曲面 |
| `get_core_mapping_table()` / `get_hmm_breakdown(delay_ms)` / `get_sensitivity_table()` / `get_sharpe_decay_curve()` | 各类分析表 |
| `export_all_tables()` / `close()` | 导出和关闭 |

### 核心类: `LiveStrategySelector`
| 方法 | 描述 |
|------|------|
| `on_tick(tick)` | tick驱动策略选择 |
| `_measure_delay(tick)` | 测量延迟 |
| `_find_nearest_tier(delay_ms)` | 最近档位 |
| `get_current_params()` / `get_current_delay()` | 获取当前参数/延迟 |

---

## 模块10: `diagnosis_periodic.py` (704行)

### 职责
周期性诊断服务，定期检查系统健康状态和资源所有权。

### 函数
| 函数 | 描述 |
|------|------|
| `_get_runtime_state()` | 获取运行时状态 |
| `_emit_periodic_resource_ownership_snapshot(runtime_core)` | 资源快照 |
| `reset_diagnosis_grace_period()` | 重置宽限期 |
| `run_14_contracts_periodic_diagnostic(storage, query_service)` | 14合约周期诊断 |

### 类: `ResourceOwnershipScanner`
| 方法 | 描述 |
|------|------|
| `log_resource_ownership_table(strategy_core, phase)` | 资源所有权表 |

---

## 模块11: `diagnosis_probe.py` (709行)

### 职责
合约诊断探针管理器，从环境变量加载监控合约，提供12个诊断点探针。

### 函数
| 函数 | 描述 |
|------|------|
| `is_monitored_contract(instrument_id)` | 是否监控合约（带TTL缓存） |
| `diagnose_parse_transform/diagnose_parse_failure/validate_contract_format` | 解析诊断 |
| `diagnose_format_validation/diagnose_config_file_presence/diagnose_config_loaded_status` | 配置诊断 |
| `diagnose_pre_registration/diagnose_subscription/diagnose_registration` | 订阅诊断 |
| `diagnose_id_lookup/diagnose_tick_entry/diagnose_tick_buffer/diagnose_tick_flush` | Tick管线诊断 |
| `diagnose_storage_enqueue/diagnose_async_write/diagnose_duckdb_insert` | 存储诊断 |

### 类: `DiagnosisProbeManager`
| 方法 | 描述 |
|------|------|
| `on_subscribe/on_register/on_tick_entry/on_storage_enqueue/on_async_write` | 回调入口 |
| `start_contract_watch/stop_contract_watch/record_contract_tick` | 合约监控 |
| `begin_startup_timeline/mark_startup_event/startup_step/emit_startup_summary` | 启动时间线 |

---

## 模块12: `diagnosis_service.py` (703行)

### 职责
策略诊断服务，全链路诊断和增量诊断。

### 数据类
| 类名 | 描述 |
|------|------|
| `DiagnoserConfig` | 诊断器配置 |
| `StrategyProtocol(Protocol)` | 策略协议接口 |

### 类: `DiagnosisReport`
| 方法 | 描述 |
|------|------|
| `add_breakpoint/add_warning/add_error/add_recommendation/add_log/add_metric` | 添加诊断项 |
| `to_dict()` / `get_health_score()` | 序列化和健康评分 |

### 类: `StrategyDiagnoser`
| 方法 | 描述 |
|------|------|
| `run_full_diagnosis(force_full)` | 全链路诊断 |
| `_run_incremental_diagnosis(start_time)` | 增量诊断 |
| `_diagnose_basic/_diagnose_data_source/_diagnose_instruments/_diagnose_subscriptions/_diagnose_klines/_diagnose_state` | 各层诊断 |
| `get_quick_report()` | 快速报告 |

---

## 模块13: `ds_data_writer.py` (726行)

### 职责
数据写入Mixin，提供tick/K线/合约的批量写入和增量加载。

### Mixin类: `DataWriterMixin`
| 方法 | 描述 |
|------|------|
| `_enrich_tick_option_metadata(ticks_data)` | 补齐期权元数据 |
| `build_tick_arrow_batch(tick_rows, instrument_id)` | 构建Arrow batch |
| `merge_arrow_tick_tables(tables)` / `merge_tick_task_batch(batch, info_callback)` | 合并Arrow表 |
| `batch_insert_ticks(ticks_data, instrument_id, use_arrow)` | 批量插入tick |
| `batch_insert_from_cache(cache_ticks)` | 从缓存批量插入 |
| `batch_insert_klines(klines_data)` | 批量插入K线 |
| `upsert_future_instrument/upsert_option_instrument` | upsert合约 |
| `upsert_future_product/upsert_option_product` | upsert品种 |
| `incremental_load(new_parquet_path)` / `refresh_data()` / `truncate_wal()` | 增量加载/刷新/截断WAL |

---

## 模块14: `ds_db_connection.py` (159行)

### 职责
数据库连接管理Mixin，线程本地连接+连接池+性能监控。

### Mixin类: `DBConnectionMixin`
| 方法 | 描述 |
|------|------|
| `_configure_connection(conn)` | 配置DuckDB连接参数 |
| `_safe_set_param(conn, param_name, param_value)` | 安全设置参数 |
| `_get_duckdb_memory_limit()` | 获取内存限制 |
| `_get_connection(read_only)` | 获取线程本地连接 |
| `_get_read_connection()` | 获取读连接 |
| `_mark_connection_unhealthy()` | 标记连接不健康 |
| `_is_fatal_database_error(e)` | 检测致命错误 |
| `close()` / `close_all()` | 关闭连接 |
| `_start_performance_monitor()` | 启动性能监控 |

---

## 模块15: `ds_option_sync.py` (274行)

### 职责
期权数据同步Mixin，基于ASOF JOIN将期权tick与期货tick对齐同步。

### Mixin类: `OptionSyncMixin`
| 方法 | 描述 |
|------|------|
| `_refresh_option_sync_stats()` | 刷新期权同步统计 |
| `_update_option_status_columns(conn)` | 更新期权/期货状态列 |

---

## 模块16: `ds_query_cache.py` (314行)

### 职责
查询缓存Mixin，提供SQL查询+MD5缓存+价格/K线便捷查询。

### Mixin类: `QueryCacheMixin`
| 方法 | 描述 |
|------|------|
| `on_tick(symbol, price, timestamp, ...)` | tick更新缓存 |
| `query(sql, params, arrow, raise_on_error, use_cache)` | 执行查询（带缓存） |
| `get_latest_price(symbol)` / `batch_get_latest_prices(symbols)` | 最新价格 |
| `get_time_range(instrument_id, start, end, columns)` | 时间范围查询 |
| `get_daily_aggregates(start_date, end_date)` / `get_symbol_daily_ohlc(symbol, start_date, end_date)` | 日聚合 |
| `get_kline_range/get_latest_klines/get_kline_count/get_kline_stats` | K线查询 |
| `explain(sql)` / `clear_cache()` | 执行计划/清缓存 |

---

## 模块17: `ds_realtime_cache.py` (463行)

### 职责
实时价格缓存，分片锁+最新价格+tick序列+内存K线生成。

### 核心类: `RealTimeCache`
| 方法 | 描述 |
|------|------|
| `__init__(max_recent_ticks, wal_path, flush_windows)` | 初始化 |
| `set_params_service(params_service)` | 设置ParamsService并回放暂存tick |
| `update_tick(symbol, price, timestamp, ...)` | 更新tick |
| `get_latest_price(symbol)` | 获取最新价格 |
| `get_recent_ticks(symbol, count)` | 获取近期tick |
| `get_spread(symbol)` / `has_symbol(symbol)` | 价差/品种检查 |
| `is_in_flush_window(check_time)` | 是否在刷盘窗口 |
| `drain_all_ticks(start_time, end_time)` / `restore_ticks(ticks)` | 排出/恢复tick |
| `get_kline_from_memory(symbol, count, period_seconds)` | 内存K线生成 |
| `get_all_active_symbols()` / `get_intraday_stats()` / `get_memory_stats()` | 统计信息 |
| `clear_all()` / `clear_latest_ticks()` / `reset_stats()` | 清理/重置 |

### 模块级函数
| 函数 | 描述 |
|------|------|
| `get_realtime_cache(**kwargs)` | 单例工厂 |

---

## 模块18: `ds_schema_manager.py` (470行)

### 职责
Schema管理Mixin，建表/索引/视图/数据回填/兼容性迁移。

### Mixin类: `SchemaManagerMixin`
| 方法 | 描述 |
|------|------|
| `_load_or_create_table()` | 加载Parquet或创建空表 |
| `_create_empty_table(conn)` | 创建空表 |
| `_create_metadata_tables()` | 创建元数据表 |
| `_ensure_ticks_raw_schema(conn)` | 兼容旧版Schema |
| `_backfill_shard_key(conn)` / `_backfill_option_metadata(conn)` / `_execute_backfill_batch(conn, batch)` | 数据回填 |
| `_cleanup_legacy_tables(conn)` | 清理遗留表 |
| `_create_indexes_and_views()` | 创建索引和视图 |

---

## 模块19: `event_bus.py` (865行)

### 职责
事件总线，发布-订阅模式，支持优先级/过滤/限流/持久化/NACK。

### 核心类: `EventBus`
| 方法 | 描述 |
|------|------|
| `__init__(max_workers, ...)` | 初始化 |
| `subscribe(event_type, callback, priority, filter_func)` | 订阅 |
| `unsubscribe(event_type, callback)` | 取消订阅 |
| `publish(event, async_mode, force, priority)` | 发布 |
| `get_event_history(event_type, limit)` / `clear_history()` | 事件历史 |
| `get_subscriber_count(event_type)` / `list_event_types()` / `has_subscribers(event_type)` | 订阅信息 |
| `on_publish(callback)` / `remove_publish_callback(callback)` | 发布回调 |
| `get_stats()` / `get_pending_count()` | 统计 |
| `shutdown(wait)` | 关闭 |
| `warmup_cache()` | 缓存预热 |

### 辅助类
| 类名 | 描述 |
|------|------|
| `RateLimiter` | 令牌桶限流器（rate, capacity, acquire） |
| `BaseEvent` / `TickEvent` / `KLineEvent` / `SignalEvent` / `OrderEvent` / `PositionEvent` / `RiskEvent` | 事件类型 |

---

## 模块20: `greeks_calculator.py` (1160行)

### 职责
Greeks计算引擎，BS模型+二叉树+蒙特卡洛+IV计算+交易日历+组合Greeks+风险限额。

### 模块级函数
| 函数 | 描述 |
|------|------|
| `_norm_cdf(x)` / `_norm_pdf(x)` | 标准正态CDF/PDF |
| `_bs_price(S, K, T, r, q, sigma, option_type)` | BS定价 |
| `_bs_greeks(S, K, T, r, q, sigma, option_type)` | BS希腊字母 |

### 类: `IVCalculator`
| 方法 | 描述 |
|------|------|
| `implied_volatility(market_price, S, K, T, r, q, option_type, ...)` | Newton-Raphson IV计算 |

### 类: `BinomialTreePricer`
| 方法 | 描述 |
|------|------|
| `price(S, K, T, r, q, sigma, option_type, steps, american)` | 二叉树定价 |
| `price_with_greeks(S, K, T, ...)` | 二叉树+Greeks（数值微分） |

### 类: `MonteCarloPricer`
| 方法 | 描述 |
|------|------|
| `price(S, K, T, r, q, sigma, option_type, ...)` | MC定价 |
| `price_with_confidence(S, K, T, ...)` | MC+置信区间 |

### 类: `TradingCalendar`
| 方法 | 描述 |
|------|------|
| `add_holiday(d)` / `is_trading_day(d)` / `trading_days_between(start, end)` / `time_to_expiry(expiry_date, use_trading_days)` | 交易日历 |

### 核心类: `GreeksCalculator`
| 方法 | 描述 |
|------|------|
| `__init__(config_path)` / `_load_config(path)` / `_set_defaults()` | 初始化 |
| `set_contract_multiplier/set_dividend_yield/set_risk_free_rate/set_update_strategy/set_trading_calendar/use_trading_days` | 配置方法 |
| `cache_option_info(instrument_id, info)` | 缓存期权信息 |
| `calculate_and_update(instrument_id, ...)` | 计算并更新Greeks |
| `update_greeks_from_tick(instrument_id, ...)` | tick驱动更新 |
| `get_greeks(instrument_id)` / `get_iv(instrument_id)` / `set_iv(instrument_id, iv)` | 获取/设置Greeks/IV |
| `get_portfolio_greeks(positions)` | 组合Greeks |
| `get_product_summary(product, ...)` | 产品汇总 |
| `check_risk_limits(positions, ...)` | 风险限额检查 |
| `get_stats()` / `clear_cache()` | 统计/清缓存 |

---

## 模块21: `hft_enhancements.py` (231行)

### 职责
高频增强引擎，统一编排7大增强模块（信号质量/订单流/微结构/波动率/Greeks/仓位/时间）。

### 核心类: `HFTEnhancementEngine`
| 方法 | 描述 |
|------|------|
| `__init__(config)` | 初始化七大增强模块 |
| `on_tick_enhanced(instrument_id, price, volume, direction, ...)` | 统一tick增强入口 |
| `get_all_stats()` | 获取所有模块统计 |

### 模块级函数
| 函数 | 描述 |
|------|------|
| `get_hft_engine(config)` | 全局单例获取 |

---

## 模块22: `main.py` (51行)

### 职责
策略包入口，懒加载导入机制。

### 函数
| 函数 | 描述 |
|------|------|
| `__getattr__(name)` | 懒加载导入，从`_LAZY_IMPORTS`映射表按需加载 |

---

## 模块23: `maintenance_service.py` (765行)

### 职责
存储维护服务，启动检查/周期诊断/ID序列/KV存储/元数据回填。

### 核心类: `StorageMaintenanceService`
| 方法 | 描述 |
|------|------|
| `run_startup_checks_fast_path(conn)` | 快路径启动检查 |
| `run_startup_checks(conn)` | 全量启动检查 |
| `run_periodic_diagnostic(conn)` | 2605合约诊断 |
| `ensure_instrument_id_sequence()` | 确保序列存在 |
| `reserve_next_global_id()` | 预留全局ID |
| `sync_instrument_id_sequence()` | 同步序列上界 |
| `get_kv_value(key)` / `set_kv_value(key, value)` | KV存储读写 |
| `backfill_metadata_exchange()` | 回填交易所信息 |
| `repair_option_underlying_product_references()` | 修复期权标的引用 |
| `ensure_option_product_catalog()` | 补齐期权品种 |
| `drop_empty_instrument_tables()` | 删除空表 |
| `recover_orphan_option_metadata()` | [废弃] 孤儿期权元数据恢复 |

---

## 模块24: `order_flow_analyzer.py` (976行)

### 职责
订单流微观结构分析器，逐笔成交/订单簿/CVD/OFI/VWAP/综合评估。

### 数据类
| 类名 | 描述 |
|------|------|
| `MicrostructureConfig` | 微观结构配置 |
| `FootprintBar` | Footprint K线 |

### 核心类: `ProductMicroData`
| 方法 | 描述 |
|------|------|
| `add_trade(price, volume, direction, timestamp)` | 添加成交 |
| `update_depth(bids, asks, timestamp)` | 更新订单簿 |
| `check_cvd_divergence(lookback_seconds)` | CVD背离检测 |
| `calc_instant_imbalance(depth_levels)` | 即时失衡 |
| `calc_ofi(lookback_seconds)` | 订单流失衡指数 |
| `calc_multi_timeframe_ofi(timeframes)` | 多时间帧OFI |
| `get_anchor_vwap(anchor_time, ...)` | 锚定VWAP |
| `evaluate_execution(order_price, order_volume, side)` | 执行评估 |
| `get_composite_assessment(lookback_seconds, ...)` | 综合评估 |

### 核心类: `MicrostructureAnalyzer`
| 方法 | 描述 |
|------|------|
| `on_trade(instrument_id, price, volume, direction, ...)` | 逐笔成交入口 |
| `on_depth(instrument_id, bids, asks, ...)` | 订单簿快照入口 |
| `get_cvd/get_cvd_divergence/get_instant_imbalance/get_ofi/get_composite_assessment` | 查询接口 |
| `export_snapshot()` / `clear()` / `get_products()` | 工具方法 |

---

## 模块25: `order_flow_bridge.py` (648行)

### 职责
订单流桥接服务+套利检测+做市商防御。

### 核心类: `OrderFlowBridge`
| 方法 | 描述 |
|------|------|
| `on_tick_feed(instrument_id, price, volume, ...)` | tick数据输入 |
| `on_depth_feed(instrument_id, bids, asks, ...)` | 深度数据输入 |
| `get_flow_consistency(product)` | 流一致性查询 |
| `get_composite_assessment(product, lookback_seconds)` | 综合评估 |
| `get_volume_weighted_imbalance(product)` | 成交量加权失衡 |
| `get_smart_money_flow(product)` | 聪明钱流 |
| `detect_arbitrage(instrument_id, current_price, ...)` | 套利检测 |

### 辅助类
| 类名 | 描述 |
|------|------|
| `MicrostructureArbitrageDetector` | 微结构套利检测器 |
| `CrossContractArbitrageDetector` | 跨合约套利检测器 |
| `SweepDetector` | 扫单检测器 |
| `MarketMakerDefenseEngine` | 做市商防御引擎 |
| `DefensiveOrder` / `ArbitrageOpportunity` | 数据类 |
| `OrderDefenseType(Enum)` | 防御类型枚举 |

---

## 模块26: `order_service.py` (1374行)

### 职责
订单服务核心，下单/撤单/成交回报/追单/虚拟仓位/信号日报。

### 核心类: `OrderService`
| 方法 | 描述 |
|------|------|
| `bind_platform_apis(insert_order_func, cancel_order_func)` | 绑定平台API |
| `send_order(instrument_id, volume, price, direction, ...)` | 下单 |
| `cancel_order(order_id)` / `cancel_all_pending()` | 撤单 |
| `on_trade_update(trade_data)` | 成交回报 |
| `check_pending_orders()` | 检查超时订单 |
| `execute_by_ranking(targets, direction, action)` | 按排名执行 |
| `persist_close_event(order_id, close_reason, pnl)` | 平仓事件持久化 |
| `on_close_signal(instrument_id, signal_type, close_price)` | 平仓信号 |
| `get_virtual_positions/get_virtual_position_summary` | 虚拟仓位查询 |
| `generate_daily_signal_report(date)` | 日报 |
| `enable_hft_enhancements/send_order_split/send_defensive_order` | HFT增强 |

### 拆单类
| 类名 | 描述 |
|------|------|
| `SmartOrderSplitter` | 智能拆单（plan_order_split） |
| `IcebergOrderSplitter` | 冰山订单（split） |
| `TWAPSplitter` | TWAP时间加权拆分（split） |

---

## 模块27: `params_service.py` (1603行)

### 职责
参数服务核心，参数管理+合约缓存+属性矩阵+输出控制+观察者+环境变量。

### 核心类: `ParamsService`
| 方法 | 描述 |
|------|------|
| `init_instrument_cache()` | 初始化合约缓存 |
| `cache_instrument_info(instrument_id, info)` | 写入缓存 |
| `get_internal_id/instrument_meta/instrument_meta_by_id` | ID查询 |
| `validate_contracts_loaded(contract_ids, ...)` | 合约加载验证 |
| `load_instrument_list(params, source)` | 加载合约列表 |
| `load_caches_from_db(data_service)` | 从DB加载缓存 |
| `load_params(path, force)` / `save_params(path, params)` / `force_refresh(path)` | 参数加载/保存/刷新 |
| `get/set/get_int/get_float/get_bool/get_str/get_list/get_dict` | 参数访问 |
| `update(params, notify)` | 批量更新 |
| `add_observer/remove_observer/_notify_observers` | 观察者管理 |
| `eval_dynamic_param(key, context)` | 动态参数计算（eval） |
| `get_output_config/should_output/invalidate_output_config` | 输出控制 |

### 辅助类
| 类名 | 描述 |
|------|------|
| `OutputConfig` | 输出配置 |
| `ParamObserver` | 参数观察者接口 |
| `ParamAuditObserver` | 审计观察者 |

---

## 模块28: `performance_monitor.py` (261行)

### 职责
性能监控，调用路径计数+执行时间统计。

### 核心类: `PathCounter` (单例)
| 方法 | 描述 |
|------|------|
| `enable/disable/is_enabled` | 启用/禁用 |
| `record_call(path, execution_time, exception)` | 记录调用 |
| `get_stats(format, **kwargs)` | 获取统计 |
| `reset()` | 重置 |

### 装饰器
| 函数 | 描述 |
|------|------|
| `count_call(path)` | 调用计数装饰器 |

---

## 模块29: `position_service.py` (1308行)

### 职责
持仓服务核心，持仓管理/止盈止损/尾盘平仓/超期检查/跨策略风控。

### 数据类
| 类名 | 描述 |
|------|------|
| `PositionRecord` | 持仓记录 |
| `PositionLimitConfig` | 限额配置 |
| `GreeksExposure` | Greeks敞口 |

### 核心类: `PositionService`
| 方法 | 描述 |
|------|------|
| `on_trade(trade)` | 成交回报 |
| `on_tick(tick)` | 行情检查止盈止损 |
| `set_position_limit(account_id, limit_amount, ...)` | 设置限额 |
| `get_position/get_net_position/has_position` | 查询 |
| `check_position_limit(account_id, required_amount)` | 检查限额 |
| `calculate_position_risk(instrument_id)` | 风险计算 |
| `check_all_positions()` | 检查所有持仓 |
| `_check_stop_profit/_check_stop_loss` | 止盈止损检查 |
| `_check_eod_close(now)` | 尾盘平仓 |
| `_check_time_stop(record, now)` | 超期检查 |

### 核心类: `CrossStrategyRiskGuard`
| 方法 | 描述 |
|------|------|
| `check(exposure)` | 跨策略风控检查 |

---

## 模块30: `preprocess_ticks.py` (851行)

### 职责
Tick预处理管线，向量化五态分类/订单流/Greeks计算/分钟Bar聚合/多粒度重采样。

### 函数
| 函数 | 描述 |
|------|------|
| `compute_option_state_vectorized(df)` | 向量化五态分类 |
| `compute_order_flow_vectorized(df)` | 向量化订单流指标 |
| `compute_greeks_vectorized(df)` | 向量化Greeks计算（实际为逐行循环） |
| `_implied_volatility_scalar(market_price, S, K, T, ...)` | 隐含波动率 |
| `_aggregate_ticks_to_bars(tick_df)` | Tick→分钟Bar |
| `_resample_bars_to_multiscale(df_1m, bar_length)` | 重采样 |
| `_enrich_bars(df)` | 衍生指标计算 |
| `process_symbol(symbol, tick_dir, min_date, max_date)` | 处理单品种 |
| `main_preprocess()` | 主入口 |

---

## 模块31: `product_initializer.py` (263行)

### 职责
品种初始化器，期权→标的映射+三阶段品种初始化。

### 函数
| 函数 | 描述 |
|------|------|
| `_get_option_underlying_product(option_product)` | 期权→标的期货映射 |
| `ensure_products_with_retry(data_service, max_retries)` | 三阶段品种初始化（期货→期权→关联） |

---

## 模块32: `ProductionQuantSystem.py` (275行)

### 职责
生产级量化系统编排器，组合HMM/趋势/IV-PCA/波动率/协整/生存分析/CRM。

### 核心类: `ProductionQuantSystem`
| 方法 | 描述 |
|------|------|
| `initialize(symbols)` | 初始化+注册单例 |
| `update_tick(symbol, high, low, close, return_value, iv_surface, timestamp_ms, cum_volume)` | Tick更新主入口 |
| `periodic_scan()` | 周期性协整扫描 |
| `persist_state()` / `restore_state()` | 持久化/恢复状态 |
| `get_system_status()` | 系统状态 |
| `shutdown()` | 关闭 |

---

## 模块33: `quant_core.py` (957行)

### 职责
量化核心算法，多周期趋势/IV-PCA/自适应HMM/波动率过滤/协整扫描/生存分析。

### 核心类
| 类名 | 描述 |
|------|------|
| `MultiPeriodTrendScorer` | 多周期趋势评分器（update→评分） |
| `IVSurfacePCA` | IV曲面PCA（update→主成分分析） |
| `AdaptiveHMM` | 自适应HMM（update→在线EM→状态推断） |
| `VolatilityRegimeFilter` | 波动率体制过滤（update→LOW_VOL/NORMAL/HIGH_VOL） |
| `CointegrationScanner` | 协整扫描器（add_symbol→update_price→scan→ADF检验） |
| `SurvivalAnalyzer` | 生存分析器（add_observation→fit→predict_survival→Cox模型） |

---

## 模块34: `quant_infra.py` (103行)

### 职责
量化基础设施，Numpy环形缓冲区+日志限速。

### 核心类: `NumpyRingBuffer`
| 方法 | 描述 |
|------|------|
| `append(value)` | 追加值 |
| `snapshot()` | 原子快照 |
| `sum/mean/std/last/sorted_values/percentile/to_list` | 统计方法 |

### 函数
| 函数 | 描述 |
|------|------|
| `rate_limit_log(logger, level, msg, key, min_interval)` | 日志速率限制 |

---

## 模块35: `quant_platform.py` (299行)

### 职责
量化平台基础设施，交易所时间/Tick聚Bar/原子状态/健康监控。

### 核心类
| 类名 | 描述 |
|------|------|
| `ExchangeTime` | 交易所时间（now_cst/get_trade_date/is_trading_hours/to_epoch_ms/from_epoch_ms） |
| `TickAggregator` | Tick聚Bar（update_tick→K线生成） |
| `AtomicSystemState` | 原子系统状态（capture/get_snapshot） |
| `SystemHealthMonitor` | 健康监控（record_latency/record_error/get_health_report） |

---

## 模块36: `quant_services.py` (411行)

### 职责
量化服务层，轻量持久化/热配置/JIT辅助/单例管理。

### 核心类
| 类名 | 描述 |
|------|------|
| `LightweightPersistence` | 轻量KV持久化（save/load/save_snapshot/load_latest_snapshot），基于SQLite |
| `HotConfigManager` | 热配置管理（get/register_callback/start_watching/stop_watching/update_config） |
| `NumbaJITHelper` | Numba JIT辅助（jit/warmup/is_available） |
| `SingletonManager` | 单例管理（register/get/shutdown_all） |

---

## 模块37: `query_service.py` (1432行)

### 职责
查询服务核心，合约分类/注册/K线聚合/导出/诊断。

### 核心类: `QueryService`
| 方法 | 描述 |
|------|------|
| `classify_instruments(instrument_ids)` | 分类合约 |
| `infer_exchange_from_id(instrument_id)` | 推断交易所 |
| `ensure_registered_instruments(instrument_ids)` | 确保合约注册 |
| `load_and_preregister_instruments(storage, params)` | 加载并预注册 |
| `get_active_instruments_by_product(product)` | 获取活跃合约 |
| `get_current_month_contracts(product)` / `get_next_month_contracts(product)` | 当月/下月合约 |
| `get_option_chain_for_future(future_instrument_id)` | 获取期权链 |
| `get_kline_count/get_tick_count/get_kline_range/get_tick_range` | 数据量/范围查询 |
| `export_kline_to_csv/export_tick_to_csv` | 数据导出 |
| `_diagnose_contract(instrument_id, is_future)` | 合约诊断 |

### 辅助类
| 类名 | 描述 |
|------|------|
| `LightKLine` | 轻量K线数据 |
| `_KlineAggregator` | K线聚合器（update→K线生成） |

---

## 模块38: `risk_service.py` (1183行)

### 职责
风控服务核心，交易前检查/安全元层/Greeks硬约束/断路器/日回撤/时间硬止损。

### 数据类/枚举
| 类名 | 描述 |
|------|------|
| `RiskLevel(Enum)` | 风险等级（LOW/MEDIUM/HIGH/CRITICAL） |
| `RiskCheckResult(Enum)` | 风控结果（PASS/WARNING/BLOCK/CIRCUIT_BREAK） |
| `RiskCheckResponse` | 风控响应（result/level/message/reason） |
| `PositionLimit` | 持仓限额 |
| `RiskMetrics` | 风险指标 |

### 核心类: `RiskService`
| 方法 | 描述 |
|------|------|
| `check_before_trade(signal)` | 主检查入口 |
| `_check_safety_meta_layer(signal)` | 安全元层检查 |
| `_check_rate_limit(symbol)` | 限频检查 |
| `_check_position_limit(account_id, required_amount)` | 持仓限额检查 |
| `_check_risk_ratio(signal)` | 风险比率检查 |
| `_check_greeks_limits(signal)` | Greeks硬约束 |
| `get_greeks_dashboard()` | Greeks仪表盘（含跳空模拟+PnL归因） |
| `calculate_risk_metrics()` | 风险指标计算 |

### 核心类: `SafetyMetaLayer`
| 方法 | 描述 |
|------|------|
| `on_equity_update(equity)` | 权益更新 |
| `_check_circuit_breaker(now)` | 断路器检查 |
| `_check_daily_drawdown()` | 日回撤检查 |
| `check_position_hard_time_stop(position_id, open_time, max_profit_reached)` | 持仓时间硬止损 |
| `is_trading_paused/is_new_open_blocked/is_hard_stop_triggered` | 状态查询 |
| `confirm_daily_resume()` | 人工确认恢复 |

---

## 模块39: `scheduler_service.py` (624行)

### 职责
调度服务核心，定时任务管理+市场时间服务。

### 数据类
| 类名 | 描述 |
|------|------|
| `JobInfo` | 周期任务信息 |
| `OnceJobInfo` | 一次性任务信息 |
| `JobStatus(Enum)` | 任务状态 |

### 核心类: `SchedulerService`
| 方法 | 描述 |
|------|------|
| `start()` / `stop()` / `is_running()` | 启动/停止/状态 |
| `add_job(func, interval, job_id, ...)` | 添加周期任务 |
| `add_once_job(func, delay, job_id)` | 添加一次性任务 |
| `remove_job/pause_job/resume_job/cancel_job/get_job/get_all_jobs` | 任务管理 |

### 核心类: `MarketTimeService`
| 方法 | 描述 |
|------|------|
| `add_holiday(d)` | 添加节假日 |
| `is_trading_day(target_date, holiday_dates)` | 是否交易日 |
| `is_market_open(exchange)` | 是否开盘 |

### 模块级函数
| 函数 | 描述 |
|------|------|
| `is_market_open(exchange)` / `get_market_time_service()` | 便捷函数 |

---

## 模块40: `sensitivity_analysis.py` (284行)

### 职责
参数敏感性分析，单参数扰动(OAT)+DuckDB结果存储。

### 数据类
| 类名 | 描述 |
|------|------|
| `SensitivityResult` | 敏感度结果（param_key/base_value/perturb_pct/compute_sensitivity） |

### 核心类: `SensitivityAnalyzer`
| 方法 | 描述 |
|------|------|
| `__init__(db_path, base_params, train_period, test_period, symbols)` | 初始化 |
| `run(perturb_pct, target_params, top_k, use_test)` | 运行敏感性分析 |
| `save_to_duckdb(results, db_path, table_name)` | 保存结果到DuckDB |
| `print_report(results)` | 打印报告 |

---

## 模块41: `service_container.py` (150行)

### 职责
依赖注入服务容器，管理层级化服务的注册、依赖校验和初始化顺序

### 类
| 类名 | 描述 |
|------|------|
| `ServiceContainer` | 服务容器，支持4层(L0-L3)依赖层级管理和循环依赖检测 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `__init__` | `()` | 初始化服务字典、依赖映射和初始化顺序 |
| `register` | `(name, service)` | 注册服务并校验层级依赖规则 |
| `get` | `(name)` | 获取服务实例 |
| `initialize_all` | `()` | 按依赖顺序校验所有服务（仅校验不调用init） |
| `_check_circular_dependencies` | `()` | DFS检测循环依赖 |
| `create_and_register_all_services` | `()` | 创建并注册所有服务（不完整） |

---

## 模块42: `shadow_strategy_engine.py` (884行)

### 职责
影子策略监控引擎，维护反向逻辑A和随机动作B两个影子策略，计算Alpha比率和绝对期望值，监控Alpha衰减

### 类
| 类名 | 描述 |
|------|------|
| `ShadowTradeRecord` | 影子策略交易记录dataclass |
| `AlphaMetrics` | Alpha比率监控指标dataclass |
| `ShadowParamsSnapshot` | 影子策略参数快照dataclass |
| `ShadowStrategyEngine` | 影子策略监控引擎主类 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `ShadowStrategyEngine.__init__` | `(log_dir, yaml_path, alpha_window_days, summary_interval_hours)` | 初始化引擎、加载锁定参数 |
| `_generate_trade_id` | `(shadow_type)` | 生成影子交易ID |
| `_load_and_lock_params` | `()` | 从YAML加载并锁定参数 |
| `process_shadow_a_signal` | `(market_state, instrument_id, signal_direction, price, quantity, signal_strength)` | 处理影子A反向信号 |
| `process_shadow_b_signal` | `(...)` | 处理影子B随机信号 |
| `record_master_trade` | `(...)` | 记录主策略交易 |
| `record_shadow_pnl` | `(shadow_type, trade_id, pnl, close_reason, commission)` | 记录影子策略盈亏 |
| `update_equity_curves` | `(master_equity, shadow_a_equity, shadow_b_equity)` | 更新权益曲线 |
| `_compute_sharpe` | `(returns, annualize_factor)` | 计算Sharpe比率 |
| `_compute_max_drawdown` | `(equity_curve)` | 计算最大回撤 |
| `_compute_expected_value` | `(trades)` | 计算期望值 |
| `compute_alpha_metrics` | `()` | 计算Alpha指标 |
| `process_signal` | `(...)` | 统一信号处理入口 |
| `process_signals_batch` | `(signals)` | 批量信号处理 |
| `generate_daily_summary` | `()` | 生成日汇总 |
| `generate_weekly_summary` | `()` | 生成周汇总 |
| `get_health_status` | `()` | 健康检查 |
| `get_recent_trades` | `(shadow_type, limit)` | 获取近期交易 |
| `get_shadow_strategy_engine` | `(**kwargs)` | 全局单例获取 |

---

## 模块43: `shared_utils.py` (500行)

### 职责
跨模块共享工具函数统一定义：类型标准化、精度转换、环形缓冲区、分片路由、初始化阶段管理、线程生命周期管理

### 类
| 类名 | 描述 |
|------|------|
| `RingBuffer` | 线程安全环形缓冲区 |
| `ShardRouter` | 确定性分片路由器（MD5哈希+取模） |
| `ThreadLifecycleManager` | 多服务线程启停与排水顺序管理 |
| `InitPhase` | 初始化阶段枚举 |
| `InitializationError` | 初始化阶段错误 |
| `InitStateMachine` | 初始化阶段状态机 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `normalize_option_type` | `(opt_type)` | 期权类型标准化(C→CALL, P→PUT) |
| `to_float32` | `(value)` | float32精度转换 |
| `normalize_instrument_id` | `(instrument_id)` | 移除交易所前缀 |
| `extract_product_code` | `(instrument_id)` | 提取品种代码 |
| `extract_strike_price` | `(instrument_id)` | 提取行权价 |
| `normalize_year_month` | `(year_month)` | 年月格式归一化(→YYMM) |
| `safe_int` | `(value, default)` | 安全整数转换 |
| `safe_float` | `(value, default)` | 安全浮点数转换 |
| `get_shard_router` | `(shard_count)` | 获取全局单例ShardRouter |
| `requires_phase` | `(required)` | 初始化阶段检查装饰器 |

---

## 模块44: `signal_service.py` (658行)

### 职责
信号生成服务（CQRS Command层），含冷却管理、信号过滤、HFT时序滤波、自适应阈值

### 类
| 类名 | 描述 |
|------|------|
| `SignalService` | 信号服务主类 |
| `KalmanFilter1D` | 一维卡尔曼滤波器 |
| `EMASignalFilter` | EMA信号滤波器 |
| `SignalTimingFilter` | 信号时序滤波器（Kalman+EMA双重条件） |
| `AdaptiveSignalThreshold` | 自适应信号阈值 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `SignalService.generate_signal` | `(instrument_id, signal_type, price, volume, reason, priority, cooldown_seconds)` | 生成交易信号 |
| `SignalService.get_signal_history` | `(instrument_id, limit)` | 获取信号历史 |
| `SignalService.set_cooldown` | `(instrument_id, cooldown_seconds, signal_type)` | 设置冷却时间 |
| `SignalService.run_benchmark` | `(iterations, cooldown_enabled)` | 性能基准测试 |
| `SignalService.validate_signal` | `(signal)` | 验证信号有效性 |
| `SignalService.generate_daily_signal_report` | `(date)` | 生成日信号报告 |
| `KalmanFilter1D.update` | `(measurement)` | 卡尔曼滤波更新 |
| `EMASignalFilter.update` | `(value)` | EMA滤波更新 |
| `SignalTimingFilter.filter_signal` | `(instrument_id, raw_strength)` | 信号时序过滤 |

---

## 模块45: `state_param_manager.py` (644行)

### 职责
状态驱动参数路由器，将五态分类映射为三态参数策略(correct_trending/incorrect_reversal/other)，含连续确认、最小持有期、防御性减仓

### 类
| 类名 | 描述 |
|------|------|
| `StateParamManager` | 状态参数管理器 |
| `StateTransitionEvent` | 状态转换事件枚举 |
| `TransitionPoint` | 转换点dataclass |
| `StateTransitionCapture` | 状态转换捕捉器(HFT入场窗口) |
| `StateTransitionAnalytics` | 状态转换分析(转换概率矩阵+驻留时间) |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `StateParamManager.update_state_from_width_cache` | `()` | 从width_cache更新状态（含确认机制） |
| `StateParamManager._switch_state` | `(new_state, resonance_strength, current_price)` | 执行状态切换 |
| `StateParamManager.get_params` | `(state)` | 获取当前/指定状态的参数集 |
| `StateParamManager.register_on_state_switch` | `(callback)` | 注册状态切换回调 |
| `StateTransitionCapture.on_state_change` | `(old_state, new_state, resonance_strength, current_price)` | 状态转换事件处理 |
| `StateTransitionCapture.check_position_exit` | `(instrument_id, current_price)` | 检查仓位退出 |
| `StateTransitionAnalytics.on_state_change` | `(instrument_id, old_state, new_state, timestamp)` | 记录转换统计 |
| `get_state_param_manager` | `(yaml_path, **kwargs)` | 全局单例获取 |

---

## 模块46: `storage_core.py` (1740行)

### 职责
存储核心Mixin，负责三阶段初始化、异步分片写入线程、WAL spill/replay、队列管理

### 类
| 类名 | 描述 |
|------|------|
| `_StorageCoreMixin` | 存储核心Mixin |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `__init__` | `(db_path, max_retries, retry_delay, ...)` | 三阶段初始化 |
| `_phase1_memory_init` | `(...)` | Phase1纯内存初始化 |
| `_phase2_db_init` | `(...)` | Phase2 DB连接+Schema+缓存 |
| `_phase3_runtime_init` | `()` | Phase3线程启动 |
| `_emergency_cleanup` | `()` | 紧急清理 |
| `_start_async_writer` | `()` | 启动异步分片写入线程 |
| `_async_shard_writer_loop` | `(shard_indices, batch_tasks, name, writer_idx)` | 分片写入循环 |
| `_async_writer_loop` | `(task_queue, batch_tasks, name)` | 通用写入循环 |
| `_enqueue_write` | `(func_name, *args, **kwargs)` | 入队写入任务 |
| `_flush_batch_to_db` | `(batch, writer_idx)` | 批量刷盘 |
| `_flush_sub_batch` | `(batch, writer_idx)` | 子批次刷盘 |
| `_wait_for_queue_capacity` | `(max_fill_rate, timeout_sec, source)` | 等待队列容量 |
| `process_tick` | `(tick)` | 处理Tick数据 |
| `save_external_kline` | `(kline_data)` | 保存外部K线 |
| `drain_all_queues` | `(timeout_per_queue)` | 排空所有队列 |
| `save` | `(key, data, async_mode)` | KV存储保存 |

---

## 模块47: `storage_query.py` (1358行)

### 职责
存储查询Mixin，提供READ+HELPER+INSTRUMENT+SCHEMA方法组

### 类
| 类名 | 描述 |
|------|------|
| `_StorageQueryMixin` | 存储查询Mixin |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `_validate_table_name` | `(table_name)` | 表名合法性校验 |
| `_execute_with_retry` | `(func, *args, **kwargs)` | 带重试执行 |
| `_make_instrument_info` | `(internal_id, instrument_id, instrument_type, **extra)` | 构建合约信息字典 |
| `infer_exchange_from_id` | `(instrument_id)` | 从合约ID推断交易所 |
| `_to_timestamp` | `(ts)` | 时间戳统一转换 |
| `_validate_tick` | `(tick)` | Tick数据校验 |
| `_validate_kline` | `(kline)` | K线数据校验 |
| `_normalize_tick_fields` | `(tick)` | Tick字段名标准化 |
| `_resolve_kline_provider` | `(provider)` | K线提供者探测 |
| `_estimate_kline_count` | `(history_minutes, kline_style)` | 估算K线数量(**有bug返回负数**) |
| `_normalize_kline_period` | `(kline_style)` | K线周期标准化 |
| `_get_instrument_info` | `(instrument_id)` | 获取合约信息 |
| `get_option_chain_by_future_id` | `(future_id)` | 获取期权链 |
| `_fetch_historical_kline_data` | `(provider, exchange, instrument_id, ...)` | 抓取历史K线数据 |
| `load_historical_klines` | `(instruments, history_minutes, ...)` | 加载历史K线(旧版) |
| `load_all_instruments` | `(strategy_instance, params, logger)` | 加载所有合约 |

---

## 模块48: `strategy_core_service.py` (1023行)

### 职责
策略核心服务Facade，组合Lifecycle/Instrument/Historical/TickHandler四个Mixin，含交易执行、风控检查、健康检查

### 类
| 类名 | 描述 |
|------|------|
| `StrategyCoreService` | 策略核心服务Facade |
| `StrategyParams` | 策略参数容器 |
| `Strategy2026` | 策略主类(继承BaseStrategy+UIMixin) |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `StrategyCoreService.execute_option_trading_cycle` | `()` | 执行期权交易周期 |
| `StrategyCoreService.check_position_risk` | `()` | 持仓风控检查 |
| `StrategyCoreService.on_order` | `(order_data)` | 订单回调 |
| `StrategyCoreService.on_trade` | `(trade_data)` | 成交回调 |
| `StrategyCoreService.get_health_status` | `()` | 健康检查API |
| `StrategyCoreService._check_ecosystem_exclusion` | `(targets)` | 互斥规则检查 |
| `StrategyCoreService._feed_shadow_engine` | `(targets, open_reason)` | 推送影子引擎 |
| `StrategyParams.__getattr__` | `(name)` | 参数属性访问 |
| `Strategy2026.onStart` | `()` | 平台启动回调 |
| `Strategy2026.onInit` | `()` | 平台初始化回调 |
| `Strategy2026.onStop` | `()` | 平台停止回调 |
| `Strategy2026.onTick` | `(tick)` | Tick回调 |
| `Strategy2026.onOrder` | `(order)` | 订单回调 |
| `Strategy2026.onTrade` | `(trade)` | 成交回调 |

---

## 模块49: `strategy_ecosystem.py` (685行)

### 职责
策略生态系统，三大策略协同(主策略/反向策略/震荡策略) + 资金路由 + 互斥规则 + 期望值底线

### 类
| 类名 | 描述 |
|------|------|
| `StrategySlot` | 策略槽位dataclass |
| `CapitalRoute` | 资金路由配置dataclass |
| `EcosystemTradeRecord` | 生态系统交易记录dataclass |
| `StrategyEcosystem` | 策略生态系统主类 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `StrategyEcosystem.on_state_switched` | `(old_state, new_state)` | SPM联动状态切换回调 |
| `StrategyEcosystem.route_capital` | `(active_state)` | 资金路由分配 |
| `StrategyEcosystem.switch_active_strategy` | `(new_state)` | 切换活跃策略 |
| `StrategyEcosystem.check_mutual_exclusion` | `(strategy_id, direction, instrument_id)` | 互斥规则检查 |
| `StrategyEcosystem.process_other_strategy_signal` | `(current_price, resonance_direction, ...)` | 震荡市策略信号处理 |
| `StrategyEcosystem.record_strategy_pnl` | `(strategy_id, pnl, commission)` | 记录策略盈亏 |
| `StrategyEcosystem.check_absolute_ev_bottomline` | `()` | 绝对期望值底线检查 |
| `StrategyEcosystem.resume_strategy` | `(strategy_id)` | 恢复暂停策略 |
| `get_strategy_ecosystem` | `(**kwargs)` | 全局单例获取 |

---

## 模块50: `strategy_historical.py` (908行)

### 职责
历史K线加载Mixin，管理加载启动/执行/进度跟踪，含合约过滤、提供者6级降级解析、可取消批量加载

### 类
| 类名 | 描述 |
|------|------|
| `HistoricalKlineMixin` | 历史K线加载Mixin |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `load_historical_klines_with_stop` | `(storage, instruments, history_minutes, kline_style, ...)` | 可取消的历史K线加载（模块级函数） |
| `HistoricalKlineMixin._init_historical_kline_mixin` | `()` | 初始化Mixin状态 |
| `HistoricalKlineMixin._filter_historical_month_scope` | `(instrument_ids)` | 年月范围过滤 |
| `HistoricalKlineMixin._build_historical_instruments` | `()` | 构建历史K线合约列表 |
| `HistoricalKlineMixin._resolve_historical_provider` | `()` | 6级降级解析历史数据提供者 |
| `HistoricalKlineMixin._load_historical_klines_once` | `(instruments, provider, provider_source)` | 一次性加载历史K线 |
| `HistoricalKlineMixin._start_historical_kline_load` | `(blocking)` | 启动历史K线加载 |
| `HistoricalKlineMixin._shutdown_historical_services` | `()` | 关闭历史K线服务 |
| `HistoricalKlineMixin._reset_historical_state_for_restart` | `()` | 重置状态以支持重启 |
| `HistoricalKlineMixin._check_and_start_historical_load_on_tick` | `()` | Tick回调中检查并启动加载 |

---

## 模块51: `strategy_instrument_mixin.py` (210行)

### 职责
合约加载、规范化、推导的纯逻辑方法簇（零self状态写入）

### 类
| 类名 | 描述 |
|------|------|
| `_InstrumentHelperMixin` | 合约辅助Mixin，提供合约规范化与推导方法 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `_extract_contract_year_month` | `(instrument_id: str) -> Optional[str]` | 提取合约四位年月，静态方法 |
| `_load_instruments_from_param_cache` | `(params) -> Optional[Dict]` | 从参数缓存加载合约列表 |
| `_cache_instruments_to_params` | `(params, futures_list, options_dict, source) -> None` | 缓存合约列表到参数对象 |
| `_load_instruments_from_output_files` | `() -> Optional[Dict]` | 从输出文件加载合约列表 |
| `_normalize_cached_futures` | `(futures_list) -> List[str]` | 规范化期货合约 |
| `_normalize_cached_options` | `(options_dict) -> Dict[str, List[str]]` | 规范化期权合约字典 |
| `_count_option_contracts` | `(options_dict) -> int` | 统计期权合约总数 |
| `_derive_underlying_futures_from_options` | `(options_dict) -> List[str]` | 从期权字典推导标的期货列表 |

---

## 模块52: `strategy_lifecycle_mixin.py` (1557行)

### 职责
策略生命周期状态定义与生命周期管理（初始化/启动/停止/销毁）

### 类
| 类名 | 描述 |
|------|------|
| `StrategyState` | 策略生命周期状态枚举 |
| `_LifecycleMixin` | 生命周期核心逻辑Mixin |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `_state_key` | `(state) -> str` | 状态标准化为字符串键 |
| `_state_is` | `(state, *targets) -> bool` | 状态比较工具 |
| `storage` | `(property)` | 惰性初始化storage |
| `transition_to` | `(new_state) -> bool` | 线程安全状态转换 |
| `_start_platform_subscribe_async` | `(instrument_ids) -> None` | 异步平台订阅 |
| `_platform_subscribe_worker` | `(instrument_ids) -> None` | 订阅工作线程 |
| `bind_platform_apis` | `(strategy_obj) -> None` | 绑定平台API |
| `_init_analytics_services` | `(params) -> None` | 初始化行情分析服务 |
| `_init_t_type_service_and_preload` | `(storage) -> None` | 初始化TTypeService并预加载 |
| `on_init` | `(*args, **kwargs) -> bool` | 策略初始化回调 |
| `on_start` | `() -> bool` | 策略启动回调 |
| `on_stop` | `() -> bool` | 策略停止回调 |
| `prepare_restart` | `() -> bool` | 重启准备 |
| `pause/resume/stop/destroy` | - | 生命周期控制方法 |
| `health_check` | `() -> Dict` | 健康检查 |
| `get_stats` | `() -> Dict` | 获取统计信息 |

---

## 模块53: `strategy_scheduler.py` (631行)

### 职责
APScheduler初始化、定时任务注册与生命周期管理

### 类
| 类名 | 描述 |
|------|------|
| `StrategyScheduler` | 策略调度器管理器 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `__init__` | `()` | 初始化调度器 |
| `bind_state_checker` | `(state_checker) -> None` | 绑定状态检查器 |
| `initialize` | `() -> None` | 初始化APScheduler（带重试） |
| `shutdown` | `(wait, wait_for_zero, zero_timeout) -> None` | 两阶段停止 |
| `pause_scheduler` | `() -> bool` | 冻结调度器 |
| `add_job_with_owner` | `(func, trigger, job_id, strategy_id, ...) -> None` | 统一job注册 |
| `remove_jobs_by_owner` | `(strategy_id) -> int` | 按owner移除job |
| `register_trading_jobs` | `(strategy_id, run_id, ...) -> None` | 注册交易定时任务 |
| `register_option_diagnosis_task` | `(t_type_service) -> None` | 注册期权诊断任务 |
| `register_cache_flush_task` | `(data_service) -> None` | 注册缓存刷写任务 |
| `register_14_contracts_diagnosis_task` | `(storage, query_service) -> None` | 注册重点合约诊断任务 |

---

## 模块54: `strategy_tick_handler.py` (1352行)

### 职责
Tick数据回调处理、分片缓冲、K线合成、HFT信号分发

### 类
| 类名 | 描述 |
|------|------|
| `TickHandlerMixin` | Tick数据处理Mixin |
| `PursuitPosition` | 追踪仓位数据类 |
| `DynamicPursuitEngine` | 动态追踪引擎 |
| `PyramidAddPositionEngine` | 金字塔加仓引擎 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `_init_tick_handler_mixin` | `() -> None` | 初始化Tick处理Mixin状态 |
| `on_tick` | `(tick) -> None` | Tick数据回调入口 |
| `_tick_probe_layer` | `(tick) -> str` | 探针层 |
| `_tick_check_layer` | `(instrument_id_raw) -> bool` | 检查层 |
| `_tick_dispatch_layer` | `(tick, instrument_id_raw) -> None` | 分发+统计层 |
| `_process_tick` | `(tick) -> None` | 核心Tick处理 |
| `_dispatch_tick` | `(tick, instrument_id, ...) -> None` | 统一Tick分发入口 |
| `_dispatch_tick_degraded` | `(tick, ...) -> None` | 降级分发 |
| `_flush_tick_buffer` | `() -> None` | 刷写分片缓冲 |
| `_save_tick_snapshot` | `() -> None` | 覆盖式快照 |
| `DynamicPursuitEngine.evaluate_surge` | `(...) -> Optional[Dict]` | 追踪信号评估 |
| `DynamicPursuitEngine.check_exit` | `(...) -> Optional[Dict]` | 退出检查 |
| `PyramidAddPositionEngine.calc_add_volume` | `(...) -> int` | 金字塔加仓量计算 |

---

## 模块55: `subscription_manager.py` (1363行)

### 职责
企业级订阅管理器：重试队列+退避策略+WAL异步落盘+告警回调

### 类
| 类名 | 描述 |
|------|------|
| `SubscriptionConfig` | 订阅配置数据类 |
| `SubscriptionManager` | 订阅管理器 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `classify_registered_instruments` | `(storage) -> Tuple[List, Dict]` | 按underlying_future_id分组已注册合约 |
| `inst_get` | `(inst, *keys, default) -> Any` | 兼容dict/object字段读取 |
| `SubscriptionManager.__init__` | `(data_manager, config)` | 初始化 |
| `_init_wal` | `()` | 初始化WAL文件 |
| `_rotate_wal` | `()` | WAL轮转 |
| `_write_wal_async` | `(record)` | 异步写入WAL |
| `_recover_from_wal` | `()` | 从WAL恢复 |
| `subscribe_all_instruments` | `(futures_list, options_dict) -> int` | 全量订阅 |
| `on_tick` | `(instrument_id, last_price, volume) -> None` | Tick路由入口 |
| `record_subscription/record_kline_received/record_tick_received` | - | 订阅成功跟踪 |
| `get_subscription_success_rate` | `() -> Dict` | 获取订阅成功率 |

---

## 模块56: `t_type_service.py` (443行)

### 职责
T型图服务Query层：期权宽度计算、T型图数据生成、期权状态识别

### 类
| 类名 | 描述 |
|------|------|
| `TTypeService` | T型图服务 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `__init__` | `(data_source, use_data_service)` | 初始化 |
| `initialize` | `()` | 显式初始化（空实现） |
| `mark_preload_complete` | `()` | 标记预加载完成 |
| `calculate_option_width` | `(instrument_id, ...) -> Dict` | 计算期权宽度 |
| `calculate_option_width_strength` | `(...) -> Dict` | 计算宽度强度 |
| `on_future_tick/on_future_instrument_tick/on_option_tick` | - | 行情推送入口 |
| `register_future_contract/register_option_contract` | - | 合约注册 |
| `get_width_strength_from_cache` | `(future_internal_id, months, option_type) -> int` | 缓存查询 |
| `compute_decision_score` | `(future_internal_id, ...) -> Dict` | 决策得分协同计算 |
| `get_t_type_service` | `() -> TTypeService` | 单例工厂函数 |

---

## 模块57: `task_scheduler.py` (4341行)

### 职责
量化任务调度系统：参数网格扫描+多进程回测+结果汇总

### 类
| 类名 | 描述 |
|------|------|
| `_BacktestPosition` | 回测仓位数据类 |
| `_BacktestState` | 回测状态数据类 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `validate_shadow_param_independence` | `(threshold) -> Dict` | 影子策略参数独立性验证 |
| `_resolve_tp_sl` | `(params, open_reason) -> Tuple[float, float]` | 按开仓原因解析止盈止损比例 |
| `_resolve_time_stop` | `(params, open_reason) -> float` | 解析硬时间止损 |
| `_check_state_transition` | `(bt, bar, params) -> str` | 状态转换检查 |
| `_compute_dynamic_slippage_bps` | `(price, bid_ask_spread, ...) -> float` | 动态滑点计算 |
| `_check_logic_reversal` | `(bt, bar, params) -> bool` | 逻辑反转平仓检测 |
| `_check_safety` | `(bt, bar_time, params) -> bool` | 风控安全检查 |
| `_try_open` | `(bt, bar, params, strategy_type) -> None` | 尝试开仓 |
| `_check_positions` | `(bt, bar, params) -> None` | 持仓检查（止盈止损/时间止损/EOD） |
| `run_backtest` | `(params, bar_data, train, strategy_type) -> Dict` | V7共振策略回测 |
| `run_backtest_box_extreme` | `(params, bar_data, train, strategy_type) -> Dict` | 箱体极值策略回测 |
| `run_backtest_box_spring` | `(params, bar_data, train, strategy_type) -> Dict` | 箱体弹簧策略回测 |
| `run_backtest_hft` | `(params, bar_data, train, strategy_type) -> Dict` | HFT策略回测 |

---

## 模块58: `ui_service.py` (1184行)

### 职责
UI控制功能：Tkinter界面、输出模式切换、参数编辑、回测参数编辑

### 类
| 类名 | 描述 |
|------|------|
| `UIEvent` | UI事件数据类 |
| `UIMixin` | 策略UI混入类 |
| `StrategyUI` | 独立UI控制面板 |
| `UIDiagnosisTool` | UI诊断工具 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `UIMixin._create_ui_in_main_thread` | `(root) -> None` | 主线程中创建UI |
| `UIMixin._start_output_mode_ui` | `() -> None` | 启动输出模式界面 |
| `UIMixin.set_output_mode` | `(mode) -> None` | 设置输出模式 |
| `UIMixin.set_auto_trading_mode` | `(auto) -> None` | 设置自动交易模式 |
| `UIMixin._on_param_modify_click` | `() -> None` | 参数编辑器 |
| `UIMixin._on_backtest_click` | `() -> None` | 回测参数编辑器 |
| `UIMixin._refresh_output_mode_ui_styles` | `() -> None` | 刷新UI样式 |
| `UIMixin._destroy_output_mode_ui` | `() -> None` | 销毁UI |
| `StrategyUI.start` | `() -> StrategyUI` | 启动UI |
| `StrategyUI.stop` | `(timeout) -> None` | 关闭UI |
| `UIDiagnosisTool.check_tkinter` | `() -> Dict` | 检查Tkinter可用性 |
| `UIDiagnosisTool.run_diagnosis` | `() -> Dict` | 运行完整诊断 |

---

## 模块59: `width_cache.py` (1928行)

### 职责
期权宽度强度实时缓存：五态状态机、同步虚值计数、增量更新

### 类
| 类名 | 描述 |
|------|------|
| `SortEntry` | 排序桶条目数据类 |
| `WidthStrengthCache` | 宽度强度缓存 |
| `_NoOpDiagnosisProbeManager` | 诊断管理器NoOp替代 |

### 方法/函数
| 名称 | 签名 | 描述 |
|------|------|------|
| `SortEntry.create` | `(internal_id, ...) -> SortEntry` | 创建排序条目 |
| `WidthStrengthCache.__init__` | `(tracked_option_tick_ids, params_service)` | 初始化 |
| `register_future` | `(future_internal_id, initial_price, month) -> None` | 注册期货 |
| `register_option` | `(instrument_id, underlying_product, month, ...) -> None` | 注册期权 |
| `on_future_tick` | `(future_internal_id, price, month) -> None` | 更新期货价格 |
| `on_future_instrument_tick` | `(future_internal_id, price) -> None` | 按internal_id更新期货 |
| `on_option_tick` | `(instrument_id, price, volume) -> None` | 更新期权价格 |
| `_classify_status` | `(underlying_future_id, month, opt_type, ...) -> str` | 五态分类 |
| `_update_option_direction` | `(internal_id, price) -> Optional[bool]` | 维护期权有效方向 |
| `_compute_sync_flag` | `(info, current_price, option_direction) -> bool` | 计算同步虚值标志 |
| `_set_current_status` | `(internal_id, info, new_status) -> None` | 更新当前状态 |
| `_resolve_internal_id` | `(instrument_id) -> Optional[int]` | instrument_id→internal_id转换 |
