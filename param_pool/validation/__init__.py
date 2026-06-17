# MODULE_ID: M1-162
"""param_pool.validation sub-package"""
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
