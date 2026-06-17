# MODULE_ID: M3-608
"""策略评判系统 — 策略微观行为诊断基础设施 v1.5-production"""
from __future__ import annotations

__all__ = [
    "StrategyJudgmentEngine",
    "JudgmentResult",
]


def __getattr__(name: str):
    if name == "StrategyJudgmentEngine":
        from .strategy_judgment_facade import StrategyJudgmentEngine
        return StrategyJudgmentEngine
    if name == "JudgmentResult":
        from .judgment_types import JudgmentResult
        return JudgmentResult
    if name == "JudgmentReport":
        from .judgment_types import JudgmentReport
        return JudgmentReport
    if name == "JudgmentVerdict":
        from .judgment_types import JudgmentVerdict
        return JudgmentVerdict
    if name == "CapitalScale":
        from .judgment_types import CapitalScale
        return CapitalScale
    if name == "judge_backtest_result":
        from .parameter_pool_adapter import judge_backtest_result
        return judge_backtest_result
    if name == "judge_sweep_results":
        from .parameter_pool_adapter import judge_sweep_results
        return judge_sweep_results
    if name == "BacktestIntegrationHooks":
        from .backtest_integration_hooks import BacktestIntegrationHooks
        return BacktestIntegrationHooks
    if name == "SURVIVED_THRESHOLD":
        from .judgment_types import SURVIVED_THRESHOLD
        return SURVIVED_THRESHOLD
    if name == "SURVIVED_THRESHOLD_DESCRIPTION":
        from .judgment_types import SURVIVED_THRESHOLD_DESCRIPTION
        return SURVIVED_THRESHOLD_DESCRIPTION
    _DEPRECATED_MODULES = {
        "jensen_alpha": ".statistical_validity_extensions",
        "judgment_deep_validation": "._judgment_services",
    }
    if name in _DEPRECATED_MODULES:
        import importlib
        return importlib.import_module(_DEPRECATED_MODULES[name], __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__version__ = "1.5.0"
