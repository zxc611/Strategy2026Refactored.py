# MODULE_ID: M1-184
"""param_pool/quantification/ — 代理到 l1_quantification（统一入口）"""
from __future__ import annotations

__all__ = [
    "MetaAuditEngine",
    "audit_backtest_engine_integrity",
    "ExternalSourceConfig",
    "TierConfig",
    "PerformanceTierManager",
    "get_tier_config",
    "STATES",
]


def __getattr__(name: str):
    if name in __all__:
        from param_pool import l1_quantification as _src
        return getattr(_src, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
