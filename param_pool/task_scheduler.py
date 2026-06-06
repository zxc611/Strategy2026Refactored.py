#!/usr/bin/env python3
"""task_scheduler - Facade模块 (R27-CP-08-FIX: 拆分为3个子模块，本文件保留向后兼容re-export)

原task_scheduler.py(2423行)已拆分为:
  - ts_param_grids.py: 参数网格定义 (RISK_FREE_RATE, BACKTEST_THRESHOLDS, *_GRID等)
  - ts_backtest_strategies.py: 6策略组回测函数 (_worker_task, run_cycle_resonance_backtest_sweep等)
  - ts_result_writer.py: 结果写入与质量门 (_insert_results, _execute_round, main_scheduler等)

本文件仅做re-export，确保所有 from ali2026v3_trading.param_pool.task_scheduler import XXX 继续有效。
"""
from ali2026v3_trading.param_pool.ts_param_grids import (
    P0_IRON_RULES,
    REASON_MULTIPLIERS,
    detect_rollover_gaps,
    compute_rollover_cost,
    RISK_FREE_RATE,
    BACKTEST_THRESHOLDS,
    MULTISCALE_BAR_LENGTHS,
    BAR_INTERVAL_GRID,
    KLINE_LENGTH_PARAM_GRID,
    _SUBPROCESS_NEEDED_COLS,
)
from ali2026v3_trading.param_pool.ts_backtest_strategies import (
    _prepare_df_for_subprocess,
    _worker_init,
    cleanup_global_data,
    _worker_task,
    run_cycle_resonance_backtest_sweep,
    run_cr_params_sweep,
)
from ali2026v3_trading.param_pool.ts_result_writer import (
    _validate_ddl_column_names,
    _ensure_results_table,
    _get_completed_task_ids,
    _query_minute_table,
    _load_data_for_period,
    _load_multiscale_data,
    _resample_bars_runtime,
    _interpolate_ticks_in_bar,
    _scale_params_with_bar_interval,
    run_kline_length_sweep,
    run_kline_length_deep_sweep,
    run_kline_cr_cross_sweep,
    validate_kline_length_quality_gates,
    _insert_results,
    _execute_round,
    _validate_params_via_params_service,
    build_and_save_life_dict,
    main_scheduler,
)
