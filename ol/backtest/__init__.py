# MODULE_ID: M1-152
"""param_pool.backtest sub-package"""
from __future__ import annotations

_SUBMODULES = [
    "backtest_config",
    "backtest_runner_base",
    "backtest_runner_utils",
    "backtest_runner_validation",
    "backtest_state",
    "backtest_strategy_runners",
    "_backtest_runners_hft",
    "_backtest_fidelity",
]


def __getattr__(name: str):
    if name in _SUBMODULES:
        import importlib
        return importlib.import_module(f".{name}", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
