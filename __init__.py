"""demo — 量化交易策略框架

实盘启动阶段只需要入口策略类，因此改为懒加载：
- 包导入时不再 eager import 离线回测/预计算/风险等重模块
- 访问具体子模块或类时按需导入
- InstrumentDataManager / get_instrument_data_manager 保持兼容
"""

__version__ = "1.6.0"

import importlib
from typing import Any


# 懒加载映射：属性名 -> 模块路径（None 表示属性就是模块本身）
_LAZY_EXPORTS = {
    # data 子模块
    'data_service': 'data.data_service',
    'query_service': 'data.query_service',
    'query_data_export': 'data.query_data_export',
    'query_instrument_service': 'data.query_instrument_service',
    'storage_core': 'data.storage_core',
    'db_adapter': 'data.db_adapter',
    'ds_db_connection': 'data.ds_db_connection',
    'ds_option_sync': 'data.ds_option_sync',
    'ds_schema_manager': 'data.ds_schema_manager',
    'ds_query_cache': 'data.ds_query_cache',
    'ds_realtime_cache': 'data.ds_realtime_cache',
    'ds_data_writer': 'data.ds_data_writer',
    'data_access': 'data.data_access',
    'historical_data_manager': 'data.historical_data_manager',
    'quant_infra': 'data.quant_infra',
    'quant_cointegration': 'data.quant_cointegration',
    'quant_hmm': 'data.quant_hmm',
    'quant_services': 'data.quant_services',
    'storage_query_base': 'data.storage_query_base',
    'storage_query_history': 'data.storage_query_history',
    'storage_query_instrument': 'data.storage_query_instrument',
    # infra
    'concurrent_utils': 'infra.concurrent_utils',
    'operations_api': 'infra.operations_api',
    # param_pool / precompute
    'backtest_orchestrator': 'param_pool.backtest_orchestrator',
    'ts_result_executor': 'param_pool.ts_result_executor',
    'meta_audit_passport': 'precompute.meta_audit_passport',
    'optuna_multiobjective_search': 'param_pool.optimization.optuna_multiobjective_search',
    'phase_scan': 'param_pool.optimization.phase_scan',
    '_validator_protocol': 'param_pool.validation._validator_protocol',
    # risk
    'risk_service': 'risk.risk_service',
    'risk_check_service': 'risk.risk_check_service',
    'risk_check_engine': 'risk.risk_check_engine',
    'risk_support': 'risk.risk_support',
    'RiskEngine': ('risk_engine', 'RiskEngine'),
    'RiskSnapshot': ('risk_engine', 'RiskSnapshot'),
    'LogDeduplicator': ('risk_engine', 'LogDeduplicator'),
    'AbnormalTradeDetector': ('risk_engine', 'AbnormalTradeDetector'),
    # signal
    'signal_service': 'signal.signal_service',
    'signal_generator': 'signal.signal_generator',
    # strategy
    'box_spring_executor': 'strategy.box_spring_executor',
    # strategy_judgment
    'strategy_judgment_facade': 'strategy_judgment.strategy_judgment_facade',
    'backtest_integration_hooks': 'strategy_judgment.backtest_integration_hooks',
    'parameter_pool_adapter': 'strategy_judgment.parameter_pool_adapter',
    'pnl_attribution': 'strategy_judgment.pnl_attribution',
    # 兼容导出
    'InstrumentDataManager': ('data.data_service', 'DataService'),
    'get_instrument_data_manager': ('data.data_service', 'get_data_service'),
}

__all__ = list(_LAZY_EXPORTS.keys()) + ['get_version']


def __getattr__(name: str) -> Any:
    spec = _LAZY_EXPORTS.get(name)
    if spec is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if isinstance(spec, tuple):
        mod_path, attr = spec
        mod = importlib.import_module(mod_path)
        return getattr(mod, attr)
    return importlib.import_module(spec)


def get_version() -> str:
    """获取版本号，优先从 git tag 读取，回退到 __version__。"""
    try:
        import subprocess
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=__import__("os").path.dirname(__file__),
        )
        if result.returncode == 0 and result.stdout and result.stdout.strip():
            tag = result.stdout.strip()
            if tag.startswith("v"):
                return tag[1:]
            return tag
    except Exception:
        pass
    return __version__
