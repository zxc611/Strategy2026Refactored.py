# MODULE_ID: M1-146
from __future__ import annotations

"""param_pool/ — 英文路径主目录"""

__all__ = ['run_backtest', 'validate_logic_reversal_no_future', 'CycleResonanceOutput', 'ParamTierManager', 'get_tier_config', 'STATES', 'backtest_position_manager', 'load_plr_thresholds_data']


def __getattr__(name: str):
    if name == "run_backtest":
        from .backtest_runner_base import run_backtest
        return run_backtest
    if name == "validate_logic_reversal_no_future":
        from .checks_orchestrator import validate_logic_reversal_no_future
        return validate_logic_reversal_no_future
    if name == "latin_hypercube_sample":
        from .optuna_multiobjective_search import latin_hypercube_sample
        return latin_hypercube_sample
    if name == "get_cycle_resonance_module":
        from .optimization.cycle_sharpe import get_cycle_resonance_module
        return get_cycle_resonance_module
    if name == "CycleResonanceOutput":
        from .optimization.cycle_sharpe import CycleResonanceOutput
        return CycleResonanceOutput
    if name == "ParamTierManager":
        from .adv_validation_misc import ParamTierManager
        return ParamTierManager
    if name == "evaluate_state_accuracy":
        from .backtest_state import evaluate_state_accuracy
        return evaluate_state_accuracy
    if name == "MetaAuditEngine":
        from ali2026v3_trading.precompute.meta_audit_engine import MetaAuditEngine
        return MetaAuditEngine
    if name == "audit_backtest_engine_integrity":
        from ali2026v3_trading.precompute.meta_audit_engine import MetaAuditEngine
        return MetaAuditEngine.audit_backtest_engine_integrity
    if name == "ExternalSourceConfig":
        from ali2026v3_trading.precompute._data_validation import ExternalSourceConfig
        return ExternalSourceConfig
    if name == "TierConfig":
        from ali2026v3_trading.precompute._quantification_core import TierConfig
        return TierConfig
    if name == "PerformanceTierManager":
        from ali2026v3_trading.precompute._quantification_core import PerformanceTierManager
        return PerformanceTierManager
    if name == "get_tier_config":
        from ali2026v3_trading.precompute._quantification_core import PerformanceTierManager
        return PerformanceTierManager.get_tier_config
    if name == "STATES":
        from ali2026v3_trading.precompute._quantification_core import TripleTruthAnchor
        return TripleTruthAnchor.STATES
    if name == "backtest_position_manager":
        from . import backtest_state as backtest_position_manager
        return backtest_position_manager
    if name == "validation_deep_orchestrator":
        from .validation import validation_deep_orchestrator as _mod
        return _mod
    if name == "backtest_runner_validation":
        from .backtest import backtest_runner_validation as _mod
        return _mod
    _SHIM_REDIRECTS = {
        "adv_validation_misc": ".validation.adv_validation_misc",
        "backtest_config": ".backtest.backtest_config",
        "backtest_runner_base": ".backtest.backtest_runner_base",
        "backtest_state": ".backtest.backtest_state",
        "checks_orchestrator": ".validation.checks_orchestrator",
        "cycle_resonance_module": ".optimization.cycle_sharpe",
        "optuna_multiobjective_search": ".optimization.optuna_multiobjective_search",
        "sensitivity_analysis": ".optimization.sensitivity",
        "ts_result_writer": ".ts.ts_result_writer",
        "validation_l2_hyperparams": ".validation.validation_l2_hyperparams",
    }
    if name in _SHIM_REDIRECTS:
        import importlib
        return importlib.import_module(_SHIM_REDIRECTS[name], __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def load_plr_thresholds_data() -> dict:
    from ali2026v3_trading.infra.serialization_utils import yaml_safe_load
    import os
    import logging
    _yaml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "param_configs.yaml")
    if not os.path.exists(_yaml_path):
        logging.error("[param_pool] param_configs.yaml 不存在: %s", _yaml_path)
        return {}
    try:
        with open(_yaml_path, 'r', encoding='utf-8') as f:
            data = yaml_safe_load(f)
        if not isinstance(data, dict):
            return {}
        return data.get('plr_thresholds', data)
    except Exception as e:
        logging.error("[param_pool] 加载param_configs.yaml失败: %s", e)
        return {}
