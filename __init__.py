"""ali2026v3_trading — 量化交易策略框架"""

__version__ = "1.5.0"

from ali2026v3_trading.data import data_service, query_service, query_data_export, query_instrument_service, query_kline_aggregator, storage_core, db_adapter, ds_db_connection, ds_option_sync, ds_schema_manager, ds_query_cache, ds_realtime_cache, ds_data_writer, data_access, historical_data_manager
from ali2026v3_trading.data import quant_infra, quant_cointegration, quant_hmm, quant_platform, quant_services, quant_trend_scorer, quant_volatility
from ali2026v3_trading.data import storage_query, storage_query_base, storage_query_history, storage_query_instrument, storage_wal_mixin
from ali2026v3_trading.infra import concurrent_utils, operations_api, shared_providers
from ali2026v3_trading.param_pool import backtest_orchestrator, ts_result_executor
from ali2026v3_trading.precompute import meta_audit_passport
from ali2026v3_trading.param_pool.optimization import optuna_multiobjective_search, phase_scan
from ali2026v3_trading.param_pool.validation import _validator_protocol
from ali2026v3_trading.risk import risk_service, risk_check_service, risk_check_engine, risk_priority_matrix, safety_meta_layer
from ali2026v3_trading.risk_engine import engine, snapshot, input_builder, market_risk, counterparty_risk, operational_risk, regulatory_risk, result_handler, shared_checks
from ali2026v3_trading.signal import signal_service, signal_generator
from ali2026v3_trading.strategy import box_spring_executor_helpers
from ali2026v3_trading.strategy_judgment import strategy_judgment_facade, backtest_integration_hooks, parameter_pool_adapter, pnl_attribution


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
