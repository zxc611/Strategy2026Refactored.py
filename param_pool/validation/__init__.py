"""param_pool.validation sub-package

五唯一性修复：包级 __init__.py 不需要独立 MODULE_ID，已删除原 M1-162（与 backtest_orchestrator.py 重复）。
"""
from __future__ import annotations

_SUBMODULES = [
    "adv_validation_misc",
    "checks_orchestrator",
    "statistical_validation",
    "validation_deep_orchestrator",
    "validation_l2_hyperparams",
]


def __getattr__(name: str):
    if name in _SUBMODULES:
        import importlib
        return importlib.import_module(f".{name}", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
