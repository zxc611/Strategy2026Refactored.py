"""param_pool.ts sub-package"""
from __future__ import annotations

_SUBMODULES = [
    "ts_backtest_strategies",
    "ts_result_writer",
]


def __getattr__(name: str):
    if name in _SUBMODULES:
        import importlib
        return importlib.import_module(f".{name}", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
