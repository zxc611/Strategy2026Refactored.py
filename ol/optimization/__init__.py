# MODULE_ID: M1-138
"""param_pool.optimization sub-package"""
from __future__ import annotations

_SUBMODULES = [
    "cycle_sharpe",
    "l2_optimizer",
    "optuna_multiobjective_search",
    "phase_scan",
    "sensitivity",
]


def __getattr__(name: str):
    if name in _SUBMODULES:
        import importlib
        return importlib.import_module(f".{name}", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
