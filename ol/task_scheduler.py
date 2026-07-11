# MODULE_ID: M1-190
# [M1-96] 任务调度器
# DEPRECATED: merged into backtest_config (2026-06-12)
# Lazy imports to avoid circular dependency

__all__ = [
    'run_backtest', '_load_data_for_period', 'SLIPPAGE_BPS',
    'main_scheduler', 'MAX_WORKERS', 'PREPROCESSED_DB', 'RESULTS_DB',
    'TRAIN_START', 'TEST_START',
    # 补充: 原由task_scheduler间接导出的验证符号
    'optimize_l2_params_step1', 'DEEP_VALIDATION_TIERS',
]

# DEEP_VALIDATION_TIERS 常量内联（避免从validation_deep_orchestrator触发循环导入）'
DEEP_VALIDATION_TIERS = {
    "must_run": {
        "description": "每次参数重检必跑（P0级别，约10秒）",
        "tests": ["doomed_tests", "market_friendliness", "hft_temporal_robustness",
                  "cross_strategy_correlation", "liquidity_stress", "regime_robustness",
                  "logic_transferability"],
    },
    "quarterly": {
        "description": "季度大检（P1级别，约30秒）",
        "tests": ["doomed_tests", "market_friendliness", "hft_temporal_robustness",
                  "cross_strategy_correlation", "liquidity_stress", "regime_robustness",
                  "logic_transferability"],
    },
    "annual": {
        "description": "年度全面审计（P0+P1全量，约2分钟）",
        "tests": ["hft_temporal_robustness", "cross_strategy_correlation",
                  "market_friendliness", "regime_robustness", "liquidity_stress",
                  "logic_transferability", "doomed_tests"],
    },
}


def __getattr__(name):
    if name == 'optimize_l2_params_step1':
        from ali2026v3_trading.param_pool.validation_l2_hyperparams import optimize_l2_params_step1
        return optimize_l2_params_step1
    if name == 'run_backtest':
        from ali2026v3_trading.param_pool.backtest_runner_base import run_backtest
        return run_backtest
    if name == '_load_data_for_period':
        from ali2026v3_trading.param_pool.ts_result_writer import _load_data_for_period
        return _load_data_for_period
    if name in __all__:
        import importlib
        _mod = importlib.import_module('ali2026v3_trading.param_pool.backtest_config')
        return getattr(_mod, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
